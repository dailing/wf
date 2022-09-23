"""
# this is a simple Node of the workflow
require:
    - 'a:a'
produce:
    - a1
"""


def task(a):
    return a + 1
