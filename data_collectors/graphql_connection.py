import aiohttp


class GraphQLConnection:
    def __init__(self,
                 session: aiohttp.ClientSession,
                 endpoint: str,
                 token: str):
        self.endpoint = endpoint
        self.session = session
        self.token = token
