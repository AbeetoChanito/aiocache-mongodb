import asyncio
from aiocache_mongodb import MongoDBCache

async def main():
    async with MongoDBCache() as cache:
        TEMPORARY_KEY = "temporary_key"
        TTL = 5

        print(f"Setting '{TEMPORARY_KEY}' with a TTL of {TTL} seconds...")
        await cache.set(TEMPORARY_KEY, "", ttl=TTL)

        await asyncio.sleep(60)

        exists_after_expiration = await cache.exists(TEMPORARY_KEY)
        print(f"Does '{TEMPORARY_KEY}' exist after expiration? {exists_after_expiration}")

        print(f"Setting '{TEMPORARY_KEY}' and applying a TTL of {TTL} seconds with the expire method..")
        await cache.set(TEMPORARY_KEY, "")
        await cache.expire(TEMPORARY_KEY, ttl=TTL)

        await asyncio.sleep(60)

        exists_after_expiration = await cache.exists(TEMPORARY_KEY)
        print(f"Does '{TEMPORARY_KEY}' exist after expiration? {exists_after_expiration}")

if __name__ == "__main__":
    asyncio.run(main())