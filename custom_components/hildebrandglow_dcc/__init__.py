"""The Hildebrand Glow (DCC) integration."""
from __future__ import annotations

import logging

from glowmarkt import BrightClient
import requests
import voluptuous as vol

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import ConfigEntryNotReady

from .const import DOMAIN
from .statistics import (
    DEFAULT_BACKFILL_DAYS,
    async_backfill_account,
    async_setup_statistics_import,
)

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [Platform.SENSOR, Platform.BUTTON]

SERVICE_BACKFILL_STATISTICS = "backfill_statistics"
ATTR_DAYS = "days"

BACKFILL_STATISTICS_SCHEMA = vol.Schema(
    {
        vol.Optional(ATTR_DAYS, default=DEFAULT_BACKFILL_DAYS): vol.All(
            vol.Coerce(int), vol.Range(min=1, max=3650)
        ),
    }
)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Hildebrand Glow (DCC) from a config entry."""
    hass.data.setdefault(DOMAIN, {})
    # Authenticate with the API
    try:
        glowmarkt = await hass.async_add_executor_job(
            BrightClient, entry.data["username"], entry.data["password"]
        )
    except requests.Timeout as ex:
        raise ConfigEntryNotReady(f"Timeout: {ex}") from ex
    except requests.exceptions.ConnectionError as ex:
        raise ConfigEntryNotReady(f"Cannot connect: {ex}") from ex
    except Exception as ex:  # pylint: disable=broad-except
        raise ConfigEntryNotReady(f"Unexpected exception: {ex}") from ex
    else:
        _LOGGER.debug("Successful Post to %sauth", glowmarkt.url)

    # Set API object
    hass.data[DOMAIN][entry.entry_id] = glowmarkt

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # Backfill 30-minute statistics, including any data that arrives late
    unsub = await async_setup_statistics_import(hass, glowmarkt)
    entry.async_on_unload(unsub)

    async def _async_handle_backfill(call: ServiceCall) -> None:
        """Manually backfill statistics for every configured account."""
        days = call.data[ATTR_DAYS]
        for account in hass.data[DOMAIN].values():
            hass.async_create_task(async_backfill_account(hass, account, days))

    if not hass.services.has_service(DOMAIN, SERVICE_BACKFILL_STATISTICS):
        hass.services.async_register(
            DOMAIN,
            SERVICE_BACKFILL_STATISTICS,
            _async_handle_backfill,
            schema=BACKFILL_STATISTICS_SCHEMA,
        )

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        hass.data[DOMAIN].pop(entry.entry_id)
        if not hass.data[DOMAIN]:
            hass.services.async_remove(DOMAIN, SERVICE_BACKFILL_STATISTICS)

    return unload_ok
