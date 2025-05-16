import sqlite3
from core.settings import settings

class LocalDB:
    def __init__(self):
        self.conn = sqlite3.connect(settings.localdb_file)
        self._create_table()
        self.index_second = self.read()

    def _create_table(self):
        with self.conn:
            self.conn.execute('''CREATE TABLE IF NOT EXISTS index_data
                                 (key TEXT PRIMARY KEY, value INTEGER)''')

    def read(self):
        cursor = self.conn.execute("SELECT value FROM index_data WHERE key=?", ("index_second",))
        result = cursor.fetchone()
        return result[0] if result else 0

    def write(self):
        with self.conn:
            self.conn.execute("INSERT OR REPLACE INTO index_data (key, value) VALUES (?, ?)",
                              ("index_second", self.index_second))

localdb = LocalDB()
