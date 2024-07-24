from typing import Awaitable, Callable, NamedTuple, Tuple

from pytoniq.liteclient import LiteClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

DBSession = async_sessionmaker[AsyncSession]


class HandlerArgs(NamedTuple):
    origin_db: DBSession
    result_db: DBSession
    address: str
    balance: int
    data_hash: str
    lite_client: LiteClient
    utime: int


HandlerFunction = Callable[[HandlerArgs], Awaitable[None]]
PackedHandler = Tuple[str, HandlerFunction]
