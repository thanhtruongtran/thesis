import redis
from sanic import Sanic

from src.databases.blockchain_etl import BlockchainETL
from src.databases.mongodb_cdp import MongoDBCDP
from src.databases.mongodb_dex import MongoDBDex
from src.databases.mongodb_klg import MongoDBKLG
from src.misc.log import log
from src.constants.config import RedisConfig


async def setup_db(sanic_src: Sanic):
    sanic_src.ctx.klg_db = MongoDBKLG()

    sanic_src.ctx.etl = BlockchainETL()
    sanic_src.ctx.dex_db = MongoDBDex()
    sanic_src.ctx.cdp_db = MongoDBCDP()


async def setup_cache(sanic_src: Sanic):
    sanic_src.ctx.redis = redis.from_url(RedisConfig.CONNECTION_URL)
    sanic_src.ctx.async_redis = redis.asyncio.from_url(RedisConfig.CONNECTION_URL, decode_responses=True)
    log('Setup Redis cached')
