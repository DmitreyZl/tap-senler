"""Vk tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_senler import streams


class TapSenler(Tap):
    """Senler tap class."""

    name = "tap-senler"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "token",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            title="Auth Token",
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "group_id",
            th.IntegerType,
            title="Группа в vk",
            description="Группа в vk",
        ),
        th.Property(
            "date_from",
            th.StringType,
            title="дата от",
            description="дата от",
        ),
        th.Property(
            "date_to",
            th.StringType,
            title="дата до",
            description="дата до",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.VkStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.DeliveriesGet(self),
            streams.DeliveriesStat(self),
            streams.StatSubscribeStream(self),
            streams.BotsGet(self),
            streams.BotsStat(self),
            streams.BotsGetSteps(self)
        ]


if __name__ == "__main__":
    TapSenler.cli()
