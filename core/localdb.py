import json

from core.settings import settings


class LocalDB:
    index_second: int

    def __init__(self):
        self.read()

    def read(self):
        with open(settings.lockfile) as f:
            data = json.load(f)
            self.index_second = data["index_second"]

    def write(self):
        with open(settings.lockfile, "w") as f:
            json.dump({"index_second": self.index_second}, f)


localdb = LocalDB()
