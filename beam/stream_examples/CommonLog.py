'''
Description: This class is used when we read a json and convert to this class where each attribute has data type.
             This is useful when we execute aggregation functions in beam.
'''

import typing

class CommonLog( typing.NamedTuple ):
    user_id         : str
    ip              : str
    timestamp       : str
    http_request    : str
    http_response   : int
    lat             : float
    lng             : float
    user_agent      : str
    num_bytes       : int
