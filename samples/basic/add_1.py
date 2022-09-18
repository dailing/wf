"""
# this is a simple Node of the workflow
require:
    - 'a:a'
    - 'b:b'
produce:
    - c
"""

from common import add, mul


def task(a, b):
    return mul(add(a, b), b)
