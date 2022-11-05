import random
import string


def random_string(num_length: int = 8) -> str:
    """Random string from uppercase ASCII and number 0-9
    """
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=num_length))
