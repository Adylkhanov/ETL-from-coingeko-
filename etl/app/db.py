from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def make_engine(sqlalchemy_url: str) -> Engine:
    return create_engine(sqlalchemy_url, pool_pre_ping=True, future=True)


def ping_db(engine: Engine) -> None:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
