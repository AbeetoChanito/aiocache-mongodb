from aiocache.base import BaseCache # type: ignore
from aiocache.serializers import JsonSerializer # type: ignore
from pymongo import AsyncMongoClient
import datetime
from typing_extensions import *

class MongoDBCache(BaseCache[str]):
    NAME = "MongoDBCache"

    def __init__(self,
        host: str = "localhost",
        port: int = 27017,
        database: str = "cache_db",
        collection_name: str = "cache_collection",
        **kwargs
    ):
        if "serializer" not in kwargs:
            kwargs["serializer"] = JsonSerializer()

        super().__init__(**kwargs)

        self.client: AsyncMongoClient = AsyncMongoClient(host, port)
        self.db = self.client[database]
        self.collection = self.db[collection_name]

    async def _get_expiration_date(self, ttl: Optional[Union[datetime.timedelta, float]]) -> Optional[datetime.datetime]:
        """
        Calculates the expiration date based on a TTL value.
        If ttl is None, returns None.

        :param ttl: Time to live in seconds.
        :return: A datetime object representing the expiration date or None.
        """

        if ttl is None:
            return None
        return datetime.datetime.now() + datetime.timedelta(seconds=ttl) if isinstance(ttl, float) else datetime.datetime.now() + ttl

    async def __aenter__(self) -> Self:
        await self.collection.create_index({"expiration_date": 1}, expireAfterSeconds=0)
        return self
    
    async def __aexit__(self, *_args, **_kwargs) -> None:
        await self.client.close()

    async def _get(self, key: str, encoding: Optional[str]="utf-8", _conn=None) -> Optional[str]:
        value = await self.collection.find_one({"key": key}, {"_id": 0, "value": 1})
        if encoding is None or value is None:
            return value
        return value.decode(encoding)
    
    _gets = _get

    async def _multi_get(self, keys: list[str], encoding: Optional[str]="utf-8", _conn=None) -> list[str]:
        cursor = self.collection.find({"key": {"$in": keys}})
        results = await cursor.to_list(length=None)
        if encoding is None:
            return [item["value"] for item in results]
        return [item["value"].decode(encoding) for item in results] 

    async def _set(self, key: str, value: str, ttl: Optional[Union[datetime.timedelta, float]]=None, _cas_token=None, _conn=None) -> None:
        expiration_date = await self._get_expiration_date(ttl)

        if _cas_token is not None:
            await self._cas(key, value, _cas_token, ttl)
            return

        await self.collection.update_one(
            {"key": key},
            {"$set": {"value": value, "expiration_date": expiration_date}},
            upsert=True
        )

    _add = _set

    async def _cas(self, key: str, value: str, cas_token: str, ttl: Optional[Union[datetime.timedelta, float]]=None, _conn=None) -> bool:
        expiration_date = await self._get_expiration_date(ttl)
        result = await self.collection.update_one(
            {"key": key, "value": cas_token},
            {"$set": {"value": value, "expiration_date": expiration_date}}
        )
        return result.modified_count == 1
    
    async def _multi_set(self, pairs: list[Tuple[str, str]], ttl=None, _conn=None) -> bool:
        values: Dict[str, Any] = dict(pairs)

        if ttl is not None:
            values_ttl: Dict[str, Dict[str, Union[str, datetime.datetime, None]]] = {}
            for key, value in values.items():
                values_ttl[key] = {"value": value, "expiration_date": await self._get_expiration_date(ttl)}
            values = values_ttl

        await self.collection.update_many({}, values)

        return True


    def __repr__(self) -> str: 
        return "MongoDBCache(host={}, port={}, database={}, collection={})".format(
            self.client.HOST,
            self.client.PORT,
            self.db.name,
            self.collection.name
        )

    

