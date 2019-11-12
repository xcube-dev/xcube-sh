from typing import Dict, Optional


class CursorMock(object):
    def __init__(self, tgt: Dict):
        self._tgt = tgt
        self._sql = None
        self._referer = None

    @property
    def referer(self) -> Optional[str]:
        return self._referer

    @referer.setter
    def referer(self, value: Optional[str]):
        self._referer = value

    def execute(self, sql: str):
        self._sql = sql

    def close(self):
        pass

    def commit(self):
        pass

    def fetchall(self):
        return self._tgt[self._referer]['fetchall']

    def fetchone(self):
        return self._tgt[self._referer]['fetchone']

    def rowcount(self):
        return self._tgt[self._referer]['rowcount']

    @property
    def description(self):
        return self._tgt[self._referer]['description']


class ConnectionMock(object):
    def __init__(self, tgt: dict):
        self._tgt = tgt

    def cursor(self):
        return CursorMock(tgt=self._tgt)

    def commit(self):
        pass


def connect(tgt: dict):
    print(f"Mocking connection to {tgt}")
    return ConnectionMock(tgt=tgt)
