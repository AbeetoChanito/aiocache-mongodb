import asyncio
from aiocache_mongodb import MongoDBCache

async def main():
    async with MongoDBCache() as cache:
        COUNTER_INITIAL_VALUE = 100
        COUNTER_VALUE_INCREMENT = 50

        print(f"Setting 'counter' to {COUNTER_INITIAL_VALUE}...")
        await cache.set("counter", COUNTER_INITIAL_VALUE)

        print(f"Incrementing 'counter' by {COUNTER_VALUE_INCREMENT}...")
        await cache.increment("counter", COUNTER_VALUE_INCREMENT)
        counter_value = await cache.get("counter")
        print(f"Counter value after increment: {counter_value}")


if __name__ == "__main__":
    asyncio.run(main())