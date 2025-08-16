import asyncio
from aiocache_mongodb import MongoDBCache

async def main():
    async with MongoDBCache() as cache:
        NAME = "Alice"

        print(f"Setting 'name' to '{NAME}'...")
        await cache.set(NAME, "Alice")

        exists = await cache.exists(NAME)
        print(f"Does 'name' exist? {exists}")

        name = await cache.get(NAME)
        print(f"Retrieved name: {name}")
        
        deleted = await cache.delete(NAME)
        print(f"Was 'name' deleted? {deleted}")

        exists_after_delete = await cache.exists(NAME)
        print(f"Does 'name' exist after deletion? {exists_after_delete}")

        pairs = [("age1", 50),
                 ("age2", 60)]
        print(f"Setting multiple pairs: {pairs}...")
        
        await cache.multi_set(pairs)

        keys_to_get = ["age1", "age2"]
        results = await cache.multi_get(keys_to_get)
        print(f"Retrieved results: {results}") 


if __name__ == "__main__":
    asyncio.run(main())