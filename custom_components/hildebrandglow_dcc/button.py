"""Button platform for Hildebrand Glow (DCC)."""
from __future__ import annotations

from collections.abc import Callable

from homeassistant.components.button import ButtonEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo

from .const import DOMAIN
from .statistics import DEFAULT_BACKFILL_DAYS, async_backfill_account


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: Callable
) -> bool:
    """Set up the button platform."""
    glowmarkt = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([BackfillStatisticsButton(hass, glowmarkt, entry)])
    return True


class BackfillStatisticsButton(ButtonEntity):
    """Button that triggers a manual statistics backfill for the account."""

    _attr_has_entity_name = True
    _attr_name = f"Backfill statistics ({DEFAULT_BACKFILL_DAYS} days)"
    _attr_icon = "mdi:database-refresh"

    def __init__(self, hass: HomeAssistant, glowmarkt, entry: ConfigEntry) -> None:
        """Initialize the button."""
        self._attr_unique_id = f"{entry.entry_id}-backfill-statistics"
        self.hass = hass
        self.glowmarkt = glowmarkt
        self.entry = entry

    @property
    def device_info(self) -> DeviceInfo:
        """Return device information."""
        return DeviceInfo(
            identifiers={(DOMAIN, self.entry.entry_id)},
            manufacturer="Hildebrand",
            model="Glow (DCC)",
            name="Hildebrand Glow (DCC)",
        )

    async def async_press(self) -> None:
        """Trigger a backfill covering the last DEFAULT_BACKFILL_DAYS days.

        Runs in the background (can take several minutes for a full year) -
        a persistent notification reports the result when it's done.
        """
        self.hass.async_create_task(
            async_backfill_account(self.hass, self.glowmarkt, DEFAULT_BACKFILL_DAYS)
        )
