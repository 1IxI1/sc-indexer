from base64 import b64decode, b64encode, urlsafe_b64decode, urlsafe_b64encode
from functools import wraps
from typing import Union

from bitarray.util import ba2hex, ba2int, hex2ba, int2ba
from pytoniq_core.boc import Address, Builder, Slice
from pytonlib.utils.address import detect_address


def optional_value(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if len(args) > 0 and args[0] is None:
            return None
        return func(*args, **kwargs)

    return wrapper


def b64_to_bytes(value: str):
    return b64decode(value)


def b64url_to_bytes(value: str):
    return urlsafe_b64decode(value)


def hex_to_bytes(value: str):
    return bytes.fromhex(value)


def bytes_to_b64(value: bytes):
    return b64encode(value).decode("utf8")


def bytes_to_b64url(value: bytes):
    return urlsafe_b64encode(value).decode("utf8")


def bytes_to_hex(value: bytes):
    return value.hex()


# converters
def b64_to_hex(value: str):
    return bytes_to_hex(b64_to_bytes(value))


def b64url_to_hex(value: str):
    return bytes_to_b64url(hex_to_bytes(value))


def b64url_to_b64(value: str):
    return bytes_to_b64(b64url_to_bytes(value))


def hex_to_b64(value: str):
    return bytes_to_b64(hex_to_bytes(value))


@optional_value
def hash_to_b64(b64_or_hex_hash):
    """
    Detect encoding of transactions hash and if necessary convert it to Base64.
    """
    if len(b64_or_hex_hash) == 44:
        # Hash is base64 or base64url
        if "_" in b64_or_hex_hash or "-" in b64_or_hex_hash:
            return b64url_to_b64(b64_or_hex_hash)
        return b64_or_hex_hash
    if len(b64_or_hex_hash) == 64:
        # Hash is hex
        return hex_to_b64(b64_or_hex_hash)
    raise ValueError(f"Invalid hash: '{b64_or_hex_hash}'")


# address utils
@optional_value
def address_to_raw(address: Union[str, None]) -> Union[str, None]:
    if address is None or address == "addr_none":
        return None
    try:
        raw_address = detect_address(address)["raw_form"].upper()
    except Exception:
        raise ValueError(f"Invalid address: '{address}'")
    return raw_address


def address_to_friendly(address: str, bounceable: bool):
    try:
        if bounceable:
            return detect_address(address)["bounceable"]["b64url"]
        else:
            return detect_address(address)["non_bounceable"]["b64url"]
    except Exception:
        raise ValueError(f"Invalid address: '{address}'")


hex_prefix = "0x"


# int64 <-> hex
@optional_value
def hex_to_int(value):
    value = value.lower()
    if value.startswith(hex_prefix):
        value = value[len(hex_prefix) :]
    return ba2int(hex2ba(value), signed=True)


@optional_value
def int_to_hex(value, length=64, signed=True):
    return ba2hex(int2ba(value, length=length, signed=signed))


def nanostr(a: int) -> str:
    return f"{a / 10**9} TON"


# dictionary parser utils
# TODO look for native (from pytoniq)


def uint32_key_parse(src) -> int:
    return Builder().store_bits(src).to_slice().load_uint(32)


def addr_key_parse(src) -> Address | None:
    addr = Builder().store_bits(src).to_slice().load_address()
    if type(addr) == Address:
        return addr
    return None


def addr_hash_parse(src, wc: int) -> Address | None:
    addr = (
        Builder()
        .store_uint(0b100, 3)
        .store_uint(wc, 8)
        .store_bits(src)
        .to_slice()
        .load_address()
    )
    if type(addr) == Address:
        return addr
    return None


def addr_hash_wc0_parse(src: Slice) -> Address | None:
    return addr_hash_parse(src, 0)


def bool_value_parse(src: Slice) -> bool:
    return bool(src.load_bit())


def coins_value_parse(src: Slice) -> int:
    return src.load_coins() or 0


def empty_parse(src: Slice) -> int:
    return 1


