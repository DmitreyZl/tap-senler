"""Stream type classes for tap-senler."""

from __future__ import annotations

import logging

from senlerpy import Senler, methods
import typing as t
from importlib import resources
from datetime import datetime, timedelta

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath
from tap_senler.client import SenlerStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"


# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.

class DeliveriesGet(SenlerStream):
    """Define custom stream."""

    name = "deliveries_get"
    primary_keys = ["delivery_id"]
    cont = {}

    schema = th.PropertiesList(
        th.Property(
            "delivery_id",
            th.IntegerType,
            description="The post's system ID",
        ),
        th.Property("name", th.StringType),
        th.Property("type", th.StringType),
        th.Property("status", th.StringType),
        th.Property("date", th.StringType),
        th.Property("inactive", th.IntegerType),
        th.Property("count_recipients", th.IntegerType),
        th.Property("count_send", th.IntegerType),
        th.Property("count_error", th.IntegerType),
        th.Property("count_transits", th.IntegerType),
        th.Property("count_read", th.IntegerType),
        th.Property("count_unsubscribers", th.IntegerType),
        th.Property("school", th.StringType),
        th.Property("group_id", th.IntegerType)
    ).to_dict()

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
        # params = self.config.get("params") or {}
        token = self.config.get('token')
        api = Senler(token)

        records = api(
            methods.Deliveries.get,
            vk_group_id=self.config.get('group_id'),
            count=100
        )

        items = records['items']
        offset = 100
        records_ = {'items': []}
        while len(items) == 100 or len(records_.get('items')) == 100:
            records_ = api(
                methods.Deliveries.get,
                vk_group_id=self.config.get('group_id'),
                count=100,
                offset=offset
            )
            items += records_['items']
            offset += 100

        for i in items:
            i.update({'school': self.config.get('school'),
                      'group_id': self.config.get('group_id')})

        target_date = (datetime.now() - timedelta(days=82)).replace(hour=0, minute=0, second=0)
        target_date_y = (datetime.now() - timedelta(days=54)).replace(hour=0, minute=0, second=0)

        self.cont["ids"] = [record.get('delivery_id')
                            for record in items
                            if record.get('date')
                            and (datetime.strptime(record['date'], '%d.%m.%Y %H:%M:%S') >= target_date)
                            and (datetime.strptime(record['date'], '%d.%m.%Y %H:%M:%S') < target_date_y)
        ] # Обновляем состояние
        # Логируем изменения в контексте

        self.logger.info(f"Updated context: {self.cont}")
        yield from extract_jsonpath(self.records_jsonpath, input=items)


class BotsGet(SenlerStream):
    """Define custom stream."""

    name = "bots_get"
    primary_keys = ["bot_id"]
    cont = {}

    schema = th.PropertiesList(
        th.Property(
            "bot_id",
            th.IntegerType,
            description="The post's system ID",
        ),
        th.Property("title", th.StringType),
        th.Property("date", th.StringType),
        th.Property("active", th.IntegerType),
        th.Property("published", th.IntegerType),
        th.Property("school", th.StringType),
        th.Property("group_id", th.IntegerType)
    ).to_dict()

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
        # params = self.config.get("params") or {}
        token = self.config.get('token')
        api = Senler(token)

        records = api(
            methods.Bots.get,
            count=100,
            vk_group_id=self.config.get('group_id'),
        )

        items = records['items']
        offset = 100
        records_ = {'items': []}
        while len(items) == 100 or len(records_.get('items')) == 100:
            records_ = api(
                methods.Bots.get,
                count=100,
                offset=offset,
                vk_group_id=self.config.get('group_id'),
            )
            items += records_['items']
            offset += 100

        for i in items:
            i.update({'school': self.config.get('school'),
                      'group_id': self.config.get('group_id')})

        self.cont["ids"] = [record.get('bot_id') for record in items] # Обновляем состояние
        # Логируем изменения в контексте

        self.logger.info(f"Updated context: {self.cont}")

        yield from extract_jsonpath(self.records_jsonpath, input=items)


class BotsStat(BotsGet):
    """Define custom stream."""

    name = "bots_stat"
    primary_keys = ["date", "vk_user_id", "bot_id", "step_id"]

    schema = th.PropertiesList(
        th.Property(
            "vk_user_id",
            th.IntegerType,
            description="The post's system ID",
        ),
        th.Property(
            "bot_id",
            th.IntegerType,
            description="The bot's system ID",
        ),
        th.Property("step_id", th.StringType),
        th.Property("vk_id", th.IntegerType),
        th.Property("date", th.StringType),
        th.Property("first_name", th.StringType),
        th.Property("last_name", th.StringType),
        th.Property("photo", th.StringType),
        th.Property("error", th.IntegerType),
        th.Property("error_code", th.IntegerType),
        th.Property("conversation_message_id", th.IntegerType),
        th.Property("school", th.StringType),
        th.Property("group_id", th.IntegerType),
        th.Property("is_read", th.IntegerType)
    ).to_dict()

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
        # params = self.config.get("params") or {}
        current_date = datetime.now().date()
        yesterday_date = current_date - timedelta(days=1)

        # Преобразуем дату в datetime с временем 00:00:00
        yesterday_datetime = datetime.combine(yesterday_date, datetime.min.time())
        current_datetime = datetime.combine(current_date, datetime.min.time())
        yesterday_date = yesterday_datetime.strftime('%Y-%m-%d %H:%M:%S')
        current_date = current_datetime.strftime('%Y-%m-%d %H:%M:%S')

        token = self.config.get('token')
        api = Senler(token)

        ids = self.cont.get("ids", [])
        self.logger.info(f"Get cont: {ids}")
        items = []
        for d_id in ids:
            records = api(
                methods.Deliveries.stat,
                date_from=self.config.get('date_from', str(yesterday_date)),
                date_to=self.config.get('date_to', str(current_date)),
                vk_group_id=self.config.get('group_id'),
                bot_id=d_id,
                m='get',
                count=100
            )
            items += records['items']
            while len(records['items']) != 0:
                records = api(
                    methods.Deliveries.stat,
                    date_from=self.config.get('date_from', str(yesterday_date)),
                    date_to=self.config.get('date_to', str(current_date)),
                    vk_group_id=self.config.get('group_id'),
                    bot_id=d_id,
                    m='get',
                    count=100,
                    offset_id=records['offset_id']
                )
                items += records['items']

        for i in items:
            i.update({'school': self.config.get('school'),
                      'group_id': self.config.get('group_id')})
        yield from extract_jsonpath(self.records_jsonpath, input=items)


class DeliveriesStat(DeliveriesGet):
    """Define custom stream."""

    name = "deliveries_stat"
    primary_keys = ["date", "vk_user_id", "delivery_id"]

    schema = th.PropertiesList(
        th.Property(
            "vk_user_id",
            th.IntegerType,
            description="The post's system ID",
        ),
        th.Property(
            "delivery_id",
            th.IntegerType,
            description="The group's system ID",
        ),
        th.Property("vk_id", th.IntegerType),
        th.Property("date", th.StringType),
        th.Property("first_name", th.StringType),
        th.Property("last_name", th.StringType),
        th.Property("photo", th.StringType),
        th.Property("error", th.IntegerType),
        th.Property("error_code", th.IntegerType),
        th.Property("conversation_message_id", th.IntegerType),
        th.Property("school", th.StringType),
        th.Property("group_id", th.IntegerType),
        th.Property("is_read", th.IntegerType),
        th.Property("reaction_id", th.IntegerType)
    ).to_dict()

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
        # params = self.config.get("params") or {}
        current_date = datetime.now().date()
        yesterday_date = current_date - timedelta(days=1)

        # Преобразуем дату в datetime с временем 00:00:00
        yesterday_datetime = datetime.combine(yesterday_date, datetime.min.time())
        current_datetime = datetime.combine(current_date, datetime.min.time())
        yesterday_date = yesterday_datetime.strftime('%Y-%m-%d %H:%M:%S')
        current_date = current_datetime.strftime('%Y-%m-%d %H:%M:%S')

        token = self.config.get('token')
        api = Senler(token)

        ids = [12595977, 12741374]
        self.logger.info(f"Get cont: {ids}")
        items = []
        for d_id in ids:
            records = api(
                methods.Deliveries.stat,
                date_from=self.config.get('date_from', str(yesterday_date)),
                date_to=self.config.get('date_to', str(current_date)),
                vk_group_id=self.config.get('group_id'),
                delivery_id=d_id,
                count=100
            )
            items += records['items']
            while len(records['items']) != 0:
                records = api(
                    methods.Deliveries.stat,
                    date_from=self.config.get('date_from', str(yesterday_date)),
                    date_to=self.config.get('date_to', str(current_date)),
                    vk_group_id=self.config.get('group_id'),
                    delivery_id=d_id,
                    count=100,
                    offset_id=records['offset_id']
                )
                items += records['items']

        for i in items:
            i.update({'school': self.config.get('school'),
                      'group_id': self.config.get('group_id')})
        yield from extract_jsonpath(self.records_jsonpath, input=items)


class StatSubscribeStream(SenlerStream):
    """Define custom stream."""

    name = "stat_subscribe"
    primary_keys = ["date", "vk_user_id"]
    logger = logging.getLogger('vk_api')

    schema = th.PropertiesList(
        th.Property(
            "vk_user_id",
            th.IntegerType,
            description="The group's system ID",
        ),
        th.Property("date", th.StringType),
        th.Property("first_name", th.StringType),
        th.Property("last_name", th.StringType),
        th.Property("photo", th.StringType),
        th.Property("subscription_id", th.IntegerType),
        th.Property("action", th.IntegerType),
        th.Property("ignore", th.IntegerType),
        th.Property("source", th.StringType),
        th.Property("utm_id", th.StringType),
        th.Property("utm_source", th.StringType),
        th.Property("utm_medium", th.StringType),
        th.Property("utm_campaign", th.StringType),
        th.Property("utm_content", th.StringType),
        th.Property("utm_term", th.StringType),
        th.Property("school", th.StringType),
        th.Property("group_id", th.IntegerType)
    ).to_dict()

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
        # params = self.config.get("params") or {}
        current_date = datetime.now().date()
        yesterday_date = current_date - timedelta(days=1)
        token = self.config.get('token')
        api = Senler(token)

        records = api(
            methods.Subscribers.stat_subscribe,
            date_from=self.config.get('date_from', str(yesterday_date)),
            date_to=self.config.get('date_to', str(current_date)),
            vk_group_id=self.config.get('group_id'),
            count=100
        )

        items = records['items']
        offset = 100
        records_ = {'items': []}
        while len(items) == 100 or len(records_.get('items')) == 100:
            records_ = api(
                methods.Subscribers.stat_subscribe,
                date_from=self.config.get('date_from', str(yesterday_date)),
                date_to=self.config.get('date_to', str(current_date)),
                vk_group_id=self.config.get('group_id'),
                count=100,
                offset=offset
            )
            items += records_['items']
            offset += 100

        for i in items:
            i.update({'school': self.config.get('school'),
                      'group_id': self.config.get('group_id')})

        yield from extract_jsonpath(self.records_jsonpath, input=items)
