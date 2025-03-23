"""Custom client handling, including SenlerStream base class."""

from __future__ import annotations

import typing as t

from senlerpy import Senler, methods

from singer_sdk.streams import Stream
from singer_sdk.helpers.jsonpath import extract_jsonpath

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class SenlerStream(Stream):
    """Stream class for Senler streams."""
    records_jsonpath = "$[*]"

    def get_records(
            self,
            context: Context | None,
    ) -> t.Iterable[dict]:
        """Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.

        Args:
            context: Stream partition or context dictionary.

        Raises:
            NotImplementedError: If the implementation is TODO
        """
        # Ваш токен доступа
        #params = self.config.get("params") or {}
        token = self.config.get('token')
        api = Senler(token)

        records = api(
            methods.Deliveries.stat,
            date_from=self.config.get('date_from'),
            date_to=self.config.get('date_to'),
            vk_group_id=self.config.get('group_id'),
        )

        yield from extract_jsonpath(self.records_jsonpath, input=records['items'])
