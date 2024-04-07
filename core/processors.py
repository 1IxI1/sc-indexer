from loguru import logger
from pytoniq.liteclient import LiteClient

from handlers.handler_types import DBSession
from handlers import handlers


async def call_handler(
    code_hash: str,
    origin_db: DBSession,
    result_db: DBSession,
    address: str,
    balance: int,
    data_hash: str,
    client: LiteClient,
    utime: int,
):
    if code_hash in handlers:
        handler_function = handlers[code_hash]
        await handler_function(
            origin_db, result_db, address, balance, data_hash, client, utime
        )
    else:
        pass
        logger.error(
            f"Handler not found for code hash: {code_hash}",
        )
