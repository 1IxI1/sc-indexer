from typing import NamedTuple

from loguru import logger
from pytoniq.liteclient import LiteClient

from handlers import handlers
from handlers.handler_types import DBSession, HandlerArgs


class CallHandlerArgs(NamedTuple):
    handler_args: HandlerArgs
    code_hash: str


async def call_handler(
    args: CallHandlerArgs,
):
    if args.code_hash in handlers:
        handler_function = handlers[args.code_hash]
        await handler_function(args.handler_args)
    else:
        pass
        logger.error(
            f"Handler not found for code hash: {args.code_hash}",
        )
