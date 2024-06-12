import requests


class AuthorizedRequests:
    def __init__(self, token: str, host: str):
        self._token = token
        self._headers = {"Authorization": f"Bearer {self._token}"}
        self._host = host

    def get(self, url, **kwargs):
        return requests.get(self._host + "/" + url, headers=self._headers, **kwargs)

    def post(self, url, data=None, json=None, **kwargs):
        return requests.post(self._host + "/" + url, headers=self._headers, data=data, json=json, **kwargs)
