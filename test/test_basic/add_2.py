"""
# this is a simple Node of the workflow
require:
    - 'b:a'
produce:
    - a2
"""


from .add_1 import task
