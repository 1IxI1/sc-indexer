from typing import Awaitable, Callable, Tuple

from pytoniq.liteclient import LiteClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

DBSession = async_sessionmaker[AsyncSession]

HandlerFunction = Callable[
    [DBSession, DBSession, str, int, str, LiteClient, int], Awaitable[None]
]
PackedHandler = Tuple[str, HandlerFunction]

