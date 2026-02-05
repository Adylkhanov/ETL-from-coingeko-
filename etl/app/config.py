import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:

    db: str
    user: str
    password: str
    host: str
    port: int

   
    coingecko_url: str
    vs_currency: str
    top_n: int
    timeout: int

    @property
    def sqlalchemy_url(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"


def get_settings() -> Settings:
    return Settings(
        db=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT")),
        coingecko_url=os.getenv("COINGECKO_URL"),
        vs_currency=os.getenv("VS_CURRENCY"),
        top_n=int(os.getenv("TOP_N")),
        timeout=int(os.getenv("REQUEST_TIMEOUT")),
    )
def get_env(key: str, default: str) -> str:
    return os.getenv(key, default)