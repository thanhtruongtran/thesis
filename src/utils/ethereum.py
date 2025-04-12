import itertools


def pairwise(iterable):
    """s -> (s0,s1), (s1,s2), (s2, s3), ..."""
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def convert_large_number_to_integer_with_sign(large_number, size=256):
    sign = 1 << size - 1
    number_int256 = (large_number & sign - 1) - (large_number & sign)
    return number_int256
