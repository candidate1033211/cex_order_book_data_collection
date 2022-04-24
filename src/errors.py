"""Here we include 2 errors: the first would be raised by the function handle if no data was 
                              written in 40 seconds
                              the second would be raised if the function produce couldn't subscribe
                              to the order book stream"""


class FlushingTimeoutError(Exception):
    pass


class ConnectionFailedError(Exception):
    pass