from sqlalchemy import create_engine

class EngineFactory:
    def __init__(self, db_url, pool_size=5, max_overflow=10, pool_recycle=3600, pool_pre_ping=True):
        self.db_url = db_url
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_recycle = pool_recycle
        self.pool_pre_ping = pool_pre_ping

    def get_engine(self):
        engine = create_engine(
            self.db_url,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            pool_recycle=self.pool_recycle,
            pool_pre_ping=self.pool_pre_ping
        )
        return engine
