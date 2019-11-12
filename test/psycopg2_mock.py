from typing import Dict


class CursorMock(object):
    def __init__(self, tgt: Dict):
        self._tgt = tgt
        self._sql = None

    def execute(self, sql: str):
        self._sql = sql

    def close(self):
        pass

    def commit(self):
        pass

    def fetchall(self):
        for k, v in self._tgt.items():
            if k in self._sql:
                return v['fetchall']
        return None

    def fetchone(self):
        for k, v in self._tgt.items():
            if k in self._sql:
                return v['fetchone']
        return None

    def rowcount(self):
        for k, v in self._tgt.items():
            if k in self._sql:
                return v['rowcount']
        return None

    @property
    def description(self):
        for k, v in self._tgt.items():
            if k in self._sql:
                return v['description']
        return None



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
