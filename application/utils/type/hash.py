"""
Research About Hashing functions
:research:
    - https://security.stackexchange.com/questions/211/how-to-securely-hash-passwords
"""

import hashlib
import uuid
import os
import hmac
from functools import wraps
from typing import (
    Tuple,
    Optional,
    Any,
)


def hash_dict(input_value: Any, exclude_keys: Optional[set] = None):
    """Hash values in dictionary"""
    _exclude_keys: set = exclude_keys or set()
    if isinstance(input_value, dict):
        return {
            k: hash_dict(v)
            if k not in _exclude_keys else v
            for k, v in input_value.items()
        }
    elif isinstance(input_value, (list, tuple)):
        return type(input_value)([hash_dict(i) for i in input_value])
    elif isinstance(input_value, bool):
        return input_value
    elif isinstance(input_value, (int, float)):
        input_value = str(input_value)
    elif input_value is None:
        return input_value
    return hashlib.sha512(input_value.encode('utf-8')).hexdigest()


def hash_string(input_value: str, num_length: int = 8) -> str:
    """Hash str input to number with SHA256 algorithm
    more algoritm be md5, sha1, sha224, sha256, sha384, sha512
    :usage:
        >>> hash_string('Hello World')
        '40300654'
    """
    _algorithm: str = 'sha256'
    return str(int(getattr(hashlib, _algorithm)(input_value.encode('utf-8')).hexdigest(), 16))[-num_length:]


def hash_string_with_salt(value):
    """Hash str
    """
    salt = uuid.uuid4().hex
    hashed_password = hashlib.sha512((value + salt).encode('utf-8')).hexdigest()
    return salt, hashed_password


def hash_string_builtin(value, num_length: int = 8) -> str:
    """
    :warning:
        hash does not always equal with the same input value
    """
    return abs(hash(value)) % (10 ** num_length)


def hash_new_password(password: str) -> Tuple[bytes, bytes]:
    """Hash the provided password with a randomly-generated salt and return the
    salt and hash to store in the database.

    :warning:
        - The use of a 16-byte salt and 100000 iterations of PBKDF2 match
          the minimum numbers recommended in the Python docs. Further increasing
          the number of iterations will make your hashes slower to compute,
          and therefore more secure.

    :ref:
        - https://stackoverflow.com/questions/9594125/salt-and-hash-a-password-in-python/56915300#56915300
    """
    salt = os.urandom(16)
    pw_hash = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 100000)
    return salt, pw_hash


def is_correct_password(salt: bytes, pw_hash: bytes, password: str) -> bool:
    """Given a previously-stored salt and hash, and a password provided by a user
    trying to log in, check whether the password is correct.
    :ref:
        - https://stackoverflow.com/questions/9594125/salt-and-hash-a-password-in-python/56915300#56915300
    """
    return hmac.compare_digest(
        pw_hash,
        hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 100000)
    )


def tokenize(*args, **kwargs):
    """Deterministic token (modified from dask.base)
    :usage:
        >>> tokenize([1, 2, '3'])
        '9d71491b50023b06fc76928e6eddb952'

        >>> tokenize('Hello') == tokenize('Hello')
        True
    """
    if kwargs:
        args += (kwargs,)
    try:
        return hashlib.md5(str(args).encode()).hexdigest()
    except ValueError:
        # FIPS systems: https://github.com/fsspec/filesystem_spec/issues/380
        return hashlib.md5(str(args).encode(), usedforsecurity=False).hexdigest()


def freeze(content: Any):
    """Freeze the content to immutable"""
    if isinstance(content, dict):
        return frozenset((key, freeze(value)) for key, value in content.items())
    elif isinstance(content, list):
        return tuple(freeze(value) for value in content)
    elif isinstance(content, set):
        return frozenset(freeze(value) for value in content)
    return content


def freeze_args(func):
    """Transform mutable dictionary into immutable useful to be compatible with cache.
    """
    class HashDict(dict):
        def __hash__(self):
            return hash(freeze(self))

    @wraps(func)
    def wrapped(*args, **kwargs):
        args: tuple = tuple(HashDict(arg) if isinstance(arg, dict) else arg for arg in args)
        kwargs: dict = {k: HashDict(v) if isinstance(v, dict) else v for k, v in kwargs.items()}
        return func(*args, **kwargs)
    return wrapped


if __name__ == '__main__':
    print(hash_string_with_salt('Hello World'))
    ...
