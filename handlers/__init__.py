from typing import Iterable, List, Tuple

from handlers.handler_types import HandlerFunction, PackedHandler
# from handlers.new_nominator_pool import nominator_pool_handler
from handlers.dedust_pool import dedust_pool_handler
from handlers.stonfi_pool import stonfi_pool_handler


class Handlers:
    handlers: dict[str, HandlerFunction] = {}

    def __init__(self):
        pass

    def register(self, to_add: PackedHandler | List[PackedHandler]):
        if not isinstance(to_add, list):
            to_add = [to_add]
        for handler in to_add:
            self.handlers[handler[0]] = handler[1]

    def __iter__(self):
        return iter(self.handlers)

    def __getitem__(self, code_hash: str):
        return self.handlers[code_hash]

    def keys(self):
        return self.handlers.keys()


handlers = Handlers()
# handlers.register([nominator_pool_handler, wallet_v4r2_handler, wallet_v3r2_handler])
# handlers.register(nominator_pool_handler)
handlers.register(dedust_pool_handler)
handlers.register(stonfi_pool_handler)
