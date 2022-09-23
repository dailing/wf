"""
# this is a simple Node of the workflow
require:
    - 'a1:a'
    - 'a2:b'
produce:
    - c
"""


def task(a, b):
    return a * b
