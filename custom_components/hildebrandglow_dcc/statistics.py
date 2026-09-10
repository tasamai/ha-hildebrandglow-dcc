"""Backfill 30-minute usage/cost data into Home Assistant long-term statistics.

The Glow DCC API sometimes doesn't have a smart meter's half-hourly readings yet
when we ask for them - the meter hasn't reported them to the DCC. That data
usually turns up later. Every cycle we resubmit a trailing window of readings as
external statistics (keyed by (statistic_id, hour), so resubmitting is a safe
upsert) - any hour that was missing last time gets backfilled as soon as the API
actually has it, with no persisted "last imported" cursor required.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
import logging

import requests

from homeassistant.components.persistent_notification import (
    async_create as async_notify,
)
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMeanType,
    StatisticMetaData,
)
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    get_last_statistics,
    statistics_during_period,
)
from homeassistant.const import UnitOfEnergy
from homeassistant.core import CALLBACK_TYPE, HomeAssistant
from homeassistant.helpers.event import async_call_later, async_track_time_interval
import homeassistant.util.dt as dt_util
from homeassistant.util.unit_conversion import EnergyConverter

from .const import DOMAIN
from .helpers import async_get_resource_pairs

_LOGGER = logging.getLogger(__name__)

STATISTICS_LOOKBACK = timedelta(days=7)
STATISTICS_UPDATE_INTERVAL = timedelta(hours=1)
# Give the initial sensor platform setup (which hits the same API for every
# resource's "today" totals) time to finish before we add our own requests.
INITIAL_IMPORT_DELAY = timedelta(seconds=90)

# The Glow API caps PT30M readings requests at 10 days per call; stay a day
# under that for safety margin. Manual/service backfills chunk into windows
# of this size, pausing briefly between chunks to avoid hammering the API.
MAX_PT30M_CHUNK = timedelta(days=9)
BACKFILL_CHUNK_DELAY_SECONDS = 1
DEFAULT_BACKFILL_DAYS = 365

CONSUMPTION_CLASSIFIERS = {"electricity.consumption", "gas.consumption"}
COST_CLASSIFIERS = {"electricity.consumption.cost", "gas.consumption.cost"}
STATISTICS_CLASSIFIERS = CONSUMPTION_CLASSIFIERS | COST_CLASSIFIERS

NAME_TEMPLATES = {
    "electricity.consumption": "Electricity consumption",
    "gas.consumption": "Gas consumption",
    "electricity.consumption.cost": "Electricity cost",
    "gas.consumption.cost": "Gas cost",
}


def _statistic_id(resource) -> str:
    """Return the external statistic_id for a resource."""
    classifier_slug = resource.classifier.replace(".", "_")
    resource_slug = str(resource.id).replace("-", "_").lower()
    return f"{DOMAIN}:{classifier_slug}_{resource_slug}"


def _name_for(resource, virtual_entity) -> str:
    """Return a human-readable name for the statistic."""
    base = NAME_TEMPLATES[resource.classifier]
    if virtual_entity.name is not None:
        return f"{virtual_entity.name} {base}"
    return base


def _metadata_for(resource, name: str) -> StatisticMetaData:
    """Return statistics metadata for a resource."""
    is_cost = resource.classifier in COST_CLASSIFIERS
    return StatisticMetaData(
        mean_type=StatisticMeanType.NONE,
        has_sum=True,
        name=name,
        source=DOMAIN,
        statistic_id=_statistic_id(resource),
        unit_class=None if is_cost else EnergyConverter.UNIT_CLASS,
        unit_of_measurement=None if is_cost else UnitOfEnergy.KILO_WATT_HOUR,
    )


async def _async_get_baseline(
    hass: HomeAssistant, statistic_id: str, window_start: datetime
) -> tuple[datetime, float]:
    """Return (fetch_start, running_sum_baseline) for a resubmission window.

    window_start is the start of the rolling lookback window (tz-aware, UTC,
    on the hour). We anchor on the true latest point ever written for this
    statistic_id rather than blindly using window_start, so the cumulative
    sum stays continuous even though we only resubmit a trailing window.
    """
    instance = get_instance(hass)
    last = await instance.async_add_executor_job(
        get_last_statistics, hass, 1, statistic_id, True, {"sum"}
    )
    rows = last.get(statistic_id) if last else None
    if not rows:
        return window_start, 0.0

    last_start = dt_util.utc_from_timestamp(rows[0]["start"])
    last_sum = float(rows[0].get("sum") or 0.0)

    if last_start < window_start:
        # Existing history predates the lookback window (e.g. HA was down
        # longer than the window). Resume right after it instead of
        # resetting to 0 or leaving a permanent hole.
        return last_start + timedelta(hours=1), last_sum

    stats = await instance.async_add_executor_job(
        statistics_during_period,
        hass,
        window_start - timedelta(hours=1),
        window_start,
        {statistic_id},
        "hour",
        None,
        {"sum"},
    )
    rows = stats.get(statistic_id) if stats else None
    baseline = float(rows[0]["sum"]) if rows else 0.0
    return window_start, baseline


async def _async_catchup(hass: HomeAssistant, resource) -> None:
    """Tell Hildebrand to pull the latest DCC data for a resource.

    Matches daily_data()'s pattern - non-fatal if it fails. Only meaningful
    once per resource per run, not once per historical chunk.
    """
    try:
        await hass.async_add_executor_job(resource.catchup)
    except requests.Timeout as ex:
        _LOGGER.error("Timeout: %s", ex)
    except requests.exceptions.ConnectionError as ex:
        _LOGGER.error("Cannot connect: %s", ex)
    # Can't use the RuntimeError exception from the library as it's not a subclass of Exception
    except Exception as ex:  # pylint: disable=broad-except
        if "Request failed" in str(ex):
            _LOGGER.warning(
                "Non-200 Status Code on catchup for %s: %s",
                resource.classifier,
                ex,
            )
        else:
            _LOGGER.exception("Unexpected exception: %s. Please open an issue", ex)


async def _async_fetch_half_hourly(hass: HomeAssistant, resource, t_from, t_to):
    """Fetch half-hourly readings for a resource, or [] on failure."""
    try:
        readings = await hass.async_add_executor_job(
            resource.get_readings, t_from, t_to, "PT30M", "sum", True
        )
        return readings
    except requests.Timeout as ex:
        _LOGGER.error("Timeout: %s", ex)
    except requests.exceptions.ConnectionError as ex:
        _LOGGER.error("Cannot connect: %s", ex)
    # Can't use the RuntimeError exception from the library as it's not a subclass of Exception
    except Exception as ex:  # pylint: disable=broad-except
        if "Request failed" in str(ex):
            _LOGGER.warning(
                "Non-200 Status Code fetching half-hourly readings for %s "
                "(from %s to %s): %s",
                resource.classifier,
                t_from,
                t_to,
                ex,
            )
        else:
            _LOGGER.exception("Unexpected exception: %s. Please open an issue", ex)
    return []


def _bucket_into_complete_hours(
    readings, is_cost: bool
) -> list[tuple[datetime, float]]:
    """Bucket half-hourly readings into hours, skipping any hour still missing a half.

    A half-hour that's genuinely missing (meter hasn't reported it yet) simply
    isn't in `readings` at all. Only emitting an hour once both its halves are
    present means we never submit a partial/misleading hourly total - the
    still-missing half arrives later, and the whole hour backfills correctly
    on a subsequent cycle.
    """
    by_slot: dict[datetime, float] = {
        dt_util.as_utc(timestamp): value.value for timestamp, value in readings
    }

    hour_starts = {ts.replace(minute=0, second=0, microsecond=0) for ts in by_slot}

    buckets: list[tuple[datetime, float]] = []
    for hour_start in sorted(hour_starts):
        first_half = hour_start
        second_half = hour_start + timedelta(minutes=30)
        if first_half in by_slot and second_half in by_slot:
            total = by_slot[first_half] + by_slot[second_half]
            if is_cost:
                total /= 100  # pence -> GBP, matching the Cost sensor's own conversion
            buckets.append((hour_start, total))
    return buckets


def _local_bound(when: datetime) -> datetime:
    """Convert a tz-aware UTC datetime to the naive-local, whole-second form
    the Glow API accepts for from/to (see the isoformat() note above)."""
    return dt_util.as_local(when).replace(tzinfo=None, second=0, microsecond=0)


async def _async_import_resource(
    hass: HomeAssistant, resource, virtual_entity, lookback: timedelta
) -> int:
    """Backfill statistics for a single resource over the given lookback window.

    Chunks the request into MAX_PT30M_CHUNK-sized windows (the Glow API's
    PT30M range limit), walking forward from the baseline so the cumulative
    sum stays continuous across chunks. Returns the number of hourly
    statistics rows written.
    """
    statistic_id = _statistic_id(resource)
    metadata = _metadata_for(resource, _name_for(resource, virtual_entity))
    is_cost = resource.classifier in COST_CLASSIFIERS

    window_start = (
        dt_util.utcnow().replace(minute=0, second=0, microsecond=0) - lookback
    )
    chunk_start, running_sum = await _async_get_baseline(
        hass, statistic_id, window_start
    )
    end = dt_util.utcnow().replace(second=0, microsecond=0)

    await _async_catchup(hass, resource)

    total_imported = 0
    while chunk_start < end:
        chunk_end = min(chunk_start + MAX_PT30M_CHUNK, end)
        t_from = _local_bound(chunk_start)
        t_to = _local_bound(chunk_end)

        readings = await _async_fetch_half_hourly(hass, resource, t_from, t_to)
        buckets = _bucket_into_complete_hours(readings, is_cost)

        if buckets:
            statistics: list[StatisticData] = []
            for hour_start, value in buckets:
                running_sum += value
                statistics.append(
                    StatisticData(start=hour_start, state=value, sum=running_sum)
                )
            async_add_external_statistics(hass, metadata, statistics)
            total_imported += len(statistics)

        chunk_start = chunk_end
        if chunk_start < end:
            await asyncio.sleep(BACKFILL_CHUNK_DELAY_SECONDS)

    _LOGGER.info(
        "Imported %s hourly statistics for %s (%s) over the last %s",
        total_imported,
        resource.classifier,
        statistic_id,
        lookback,
    )
    return total_imported


async def _async_import_all(
    hass: HomeAssistant, glowmarkt, lookback: timedelta = STATISTICS_LOOKBACK
) -> int:
    """Backfill statistics for every usage/cost resource on the account.

    Returns the total number of hourly statistics rows written.
    """
    total_imported = 0
    for resource, virtual_entity in await async_get_resource_pairs(hass, glowmarkt):
        if resource.classifier not in STATISTICS_CLASSIFIERS:
            continue
        try:
            total_imported += await _async_import_resource(
                hass, resource, virtual_entity, lookback
            )
        except Exception:  # pylint: disable=broad-except
            _LOGGER.exception(
                "Unexpected error importing statistics for %s (%s). Please open an issue",
                resource.classifier,
                resource.id,
            )
    return total_imported


async def async_backfill_account(
    hass: HomeAssistant, glowmarkt, days: int = DEFAULT_BACKFILL_DAYS
) -> int:
    """Manually backfill statistics for an account over the given number of days.

    Used by the backfill_statistics service and the button entity. Posts a
    persistent notification with the result, since this can take a while.
    """
    total_imported = await _async_import_all(
        hass, glowmarkt, lookback=timedelta(days=days)
    )
    async_notify(
        hass,
        f"Imported {total_imported} hourly statistics from the last {days} days.",
        title="Hildebrand Glow (DCC) backfill complete",
        notification_id=f"{DOMAIN}_backfill",
    )
    return total_imported


async def async_setup_statistics_import(
    hass: HomeAssistant, glowmarkt
) -> CALLBACK_TYPE:
    """Import historical statistics now and on a recurring schedule.

    Returns an unsub callback; register it with entry.async_on_unload so the
    periodic job stops when the config entry is unloaded or reloaded.
    """

    async def _run(_now: datetime | None = None) -> None:
        await _async_import_all(hass, glowmarkt)

    unsub_interval = async_track_time_interval(hass, _run, STATISTICS_UPDATE_INTERVAL)
    unsub_initial = async_call_later(hass, INITIAL_IMPORT_DELAY, _run)

    def _unsub() -> None:
        unsub_interval()
        unsub_initial()

    return _unsub
