BASE32ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUV"


def encode_region(val: tuple[int, int, int, int]) -> str:
    return int2base32(int("".join(map(str, val))))


def int2base32(val: int) -> str:
    res = ""
    while True:
        if val < 32:
            res += BASE32ALPHABET[val]
            break
        else:
            res += BASE32ALPHABET[val % 32]
            val //= 32
    return res[::-1]


def base32_to_int(val: str) -> int:
    res = 0
    for i, char in enumerate(reversed(val)):
        res += BASE32ALPHABET.find(char) * 32**i
    return res
