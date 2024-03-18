# from processors.wallet_v3r2 import handle_wallet_v3
# from processors.wallet_v4r2 import handle_wallet_v4

# handlers = {
#     "mj7BS8CY9rRAZMMFIiyuooAPF92oXuaoGYpwle3hDc8=": handle_nominator_pool,
#     "/rX/aCDi/w2Ug+fg1iyBfYRniftK5YDIeIZtlZ2r1cA=": handle_wallet_v4,
#     "hNr6RJ+Ypph3ibojI1gHK8D3bcRSQAKl0JGLmnXS1Zk=": handle_wallet_v3,
# }
from typing import Iterable, List, Tuple

from core.handler_types import HandlerFunction, PackedHandler
from processors.nominator_pool import nominator_pool_handler
from processors.wallet_v3r2 import wallet_v3r2_handler
from processors.wallet_v4r2 import wallet_v4r2_handler

# handlers: dict[str, HandlerFunction] = {}


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
handlers.register([nominator_pool_handler, wallet_v4r2_handler, wallet_v3r2_handler])
