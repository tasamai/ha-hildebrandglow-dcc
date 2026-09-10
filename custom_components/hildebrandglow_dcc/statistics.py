"""Backfill 30-minute usage/cost data into Home Assistant long-term statistics.

The Glow DCC API sometimes doesn't have a smart meter's half-hourly readings yet
when we ask for them - the meter hasn't reported them to the DCC. That data
usually turns up later. Every cycle we resubmit a trailing window of readings as
external statistics (keyed by (statistic_id, hour), so resubmitting is a safe
upsert) - any hour that was missing last time gets backfilled as soon as the API
actually has it, with no persisted "last imported" cursor required.
"""
from __future__ import annotations

from datetime import datetime, timedelta
import logging

import requests

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


async def _async_fetch_half_hourly(hass: HomeAssistant, resource, t_from, t_to):
    """Fetch half-hourly readings for a resource, or [] on failure."""
    # Tell Hildebrand to pull the latest DCC data before asking for readings,
    # matching daily_data()'s pattern - non-fatal if it fails.
    try:
        await hass.async_add_executor_job(resource.catchup)
    except requests.Timeout as ex:
        _LOGGER.error("Timeout: %s", ex)
    except requests.exceptions.ConnectionError as ex:
        _LOGGER.error("Cannot connect: %s", ex)
    # Can't use the RuntimeError exception from the library as it's not a subclass of Exception
    except Exception as ex:  # pylint: disable=broad-except
        if "Request failed" in str(ex):
            _LOGGER.debug("Catchup exception detail: %s", ex)
            _LOGGER.warning(
                "Non-200 Status Code on catchup. The Glow API may be experiencing "
                "issues"
            )
        else:
            _LOGGER.exception("Unexpected exception: %s. Please open an issue", ex)

    try:
        _LOGGER.debug(
            "Get half-hourly readings from %s to %s for %s",
            t_from,
            t_to,
            resource.classifier,
        )
        readings = await hass.async_add_executor_job(
            resource.get_readings, t_from, t_to, "PT30M", "sum", True
        )
        _LOGGER.debug(
            "Got %s half-hourly readings for %s between %s and %s",
            len(readings),
            resource.classifier,
            t_from,
            t_to,
        )
        return readings
    except requests.Timeout as ex:
        _LOGGER.error("Timeout: %s", ex)
    except requests.exceptions.ConnectionError as ex:
        _LOGGER.error("Cannot connect: %s", ex)
    # Can't use the RuntimeError exception from the library as it's not a subclass of Exception
    except Exception as ex:  # pylint: disable=broad-except
        if "Request failed" in str(ex):
            _LOGGER.debug("Readings exception detail: %s", ex)
            _LOGGER.warning(
                "Non-200 Status Code. The Glow API may be experiencing issues"
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


async def _async_import_resource(hass: HomeAssistant, resource, virtual_entity) -> None:
    """Backfill statistics for a single resource."""
    statistic_id = _statistic_id(resource)
    metadata = _metadata_for(resource, _name_for(resource, virtual_entity))
    is_cost = resource.classifier in COST_CLASSIFIERS

    window_start = dt_util.utcnow().replace(
        minute=0, second=0, microsecond=0
    ) - STATISTICS_LOOKBACK
    fetch_start, running_sum = await _async_get_baseline(
        hass, statistic_id, window_start
    )

    t_from = dt_util.as_local(fetch_start).replace(tzinfo=None)
    t_to = datetime.now()

    readings = await _async_fetch_half_hourly(hass, resource, t_from, t_to)
    buckets = _bucket_into_complete_hours(readings, is_cost)
    if not buckets:
        return

    statistics: list[StatisticData] = []
    for hour_start, value in buckets:
        running_sum += value
        statistics.append(StatisticData(start=hour_start, state=value, sum=running_sum))

    async_add_external_statistics(hass, metadata, statistics)
    _LOGGER.debug(
        "Imported %s hourly statistics for %s (%s)",
        len(statistics),
        resource.classifier,
        statistic_id,
    )


async def _async_import_all(hass: HomeAssistant, glowmarkt) -> None:
    """Backfill statistics for every usage/cost resource on the account."""
    for resource, virtual_entity in await async_get_resource_pairs(hass, glowmarkt):
        if resource.classifier not in STATISTICS_CLASSIFIERS:
            continue
        try:
            await _async_import_resource(hass, resource, virtual_entity)
        except Exception:  # pylint: disable=broad-except
            _LOGGER.exception(
                "Unexpected error importing statistics for %s (%s). Please open an issue",
                resource.classifier,
                resource.id,
            )


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
