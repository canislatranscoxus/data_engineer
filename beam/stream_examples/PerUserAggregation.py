'''
Description: This class is used when we read a json and convert to this class where each attribute has data type.
             This is useful when we execute aggregation functions in beam.
'''

import typing

class PerUserAggregation( typing.NamedTuple ):
    user_id         : str
    page_views      : int
    total_bytes     : int
    max_bytes       : int
    min_bytes       : int
