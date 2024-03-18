from typing import Awaitable, Callable, Tuple

from pytoniq.liteclient import LiteClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

DBSession = async_sessionmaker[AsyncSession]

HandlerFunction = Callable[
    [DBSession, DBSession, str, int, str, LiteClient], Awaitable[None]
]
PackedHandler = Tuple[str, HandlerFunction]

# contract_handlers: dict[str, HandlerFunction]  # = {}
# # decorator for smart contract handlers
# def SC_handler(code_hash: str):
#     def decorator(func: HandlerFunction):
#         contract_handlers[code_hash] = func
#         return func

#     return decorator
