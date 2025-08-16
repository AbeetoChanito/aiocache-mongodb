from aiocache.base import BaseCache # type: ignore
from aiocache.serializers import JsonSerializer # type: ignore
from pymongo import AsyncMongoClient, UpdateOne, ASCENDING
import datetime
from typing import *

class MongoDBCache(BaseCache):
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

    @staticmethod
    def _get_expiration_date(ttl: Optional[float]) -> Optional[datetime.datetime]:
        """
        Calculates the expiration date based on a TTL value.
        If ttl is None, returns None.

        :param ttl: Time to live in seconds.
        :return: A datetime object representing the expiration date or None.
        """

        if ttl is None:
            return None
        return datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=ttl)

    async def __aenter__(self) -> "MongoDBCache":
        await self.collection.create_index([("expiration_date", ASCENDING)], expireAfterSeconds=0)
        return self
    
    async def __aexit__(self, *_args: Any, **_kwargs: Any) -> None:
        await self.client.close()

    async def _get(self, key: str, encoding: Optional[str]="utf-8", _conn=None) -> Optional[Any]:
        value = await self.collection.find_one({"key": key}, {"_id": 0, "value": 1})
        if value is None:
            return None
        if encoding is None:
            return value["value"]
        return value["value"].encode(encoding)
    
    _gets = _get

    async def _multi_get(self, keys: list[str], encoding: Optional[str]="utf-8", _conn=None) -> list[Any]:
        cursor = self.collection.find({"key": {"$in": keys}})
        results = await cursor.to_list(length=None)
        if encoding is None:
            return [item["value"] for item in results]
        return [item["value"].encode(encoding) for item in results] 

    async def _set(self, key: str, value: Any, ttl: Optional[float]=None, _cas_token=None, _conn=None) -> None:
        expiration_date = self._get_expiration_date(ttl)

        if _cas_token is not None:
            await self._cas(key, value, _cas_token, ttl)
            return

        await self.collection.update_one(
            {"key": key},
            {"$set": {"value": value, "expiration_date": expiration_date}},
            upsert=True
        )

    _add = _set

    async def _cas(self, key: str, value: Any, cas_token: str, ttl: Optional[float]=None, _conn=None) -> bool:
        expiration_date = self._get_expiration_date(ttl)
        result = await self.collection.update_one(
            {"key": key, "value": cas_token},
            {"$set": {"value": value, "expiration_date": expiration_date}}
        )
        return result.modified_count == 1
    
    async def _multi_set(self, pairs: list[Tuple[str, Any]], ttl=None, _conn=None) -> bool:
        expiration_date = self._get_expiration_date(ttl)
    
        requests = []
        for key, value in pairs:
            update = {"value": value}
            if expiration_date is not None:
                update["expiration_date"] = expiration_date
            
            requests.append(
                UpdateOne(
                    {"key": key},
                    {"$set": update},
                    upsert=True
                )
            )

        result = await self.collection.bulk_write(requests)
        return result.acknowledged

    async def _exists(self, key: str, _conn=None) -> bool:
        result = await self.collection.find_one({"key": key}, {"_id": 1})
        return result is not None

    async def _increment(self, key: str, delta: int, _conn=None) -> None:
        # NOTE: We have to get and set the value to handle the serializer.
        value = await self.get(key)

        if value is None:
            await self.collection.update_one(
                {"key": key},
                {"$set": {"value": delta}},
                upsert=True
            )
            return
            
        if not isinstance(value, (int, float)):
            raise TypeError(f"Value is not incrementable") from None

        await self.set(key, value + delta)

    async def _expire(self, key: str, ttl: float, _conn=None) -> bool:
        result = None
        if ttl == 0:
            result = await self.collection.update_one({"key": key}, {"$set": {"expiration_date": None}}) 
            return result.modified_count == 1
        else:
            expiration_date = self._get_expiration_date(ttl)
            result = await self.collection.update_one({"key": key}, {"$set": {"expiration_date": expiration_date}})
            return result.modified_count == 1

    async def _delete(self, key: str, _conn=None) -> bool:
        result = await self.collection.delete_one({"key": key})
        return result.deleted_count == 1
    
    async def _clear(self, _namespace=None, _conn=None) -> bool:
        result = await self.collection.delete_many({})
        return result.acknowledged
    
    async def _raw(self, command: str, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError
    
    async def _redlock_release(self, key: str, value: Any) -> bool:
        result = await self.collection.find_one_and_delete({"key": key, "value": value})
        return result is not None

    def __repr__(self) -> str: 
        return f"MongoDBCache(host={self.client.HOST}, port={self.client.PORT}, database={self.db.name}, collection={self.collection.name})"
    