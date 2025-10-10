from sqlalchemy import create_engine
from src.config.config import POSTGRES_CONFIG
import pandas as pd


def get_postgres_engine():
    user = POSTGRES_CONFIG["user"]
    password = POSTGRES_CONFIG["password"]
    host = POSTGRES_CONFIG["host"]
    port = POSTGRES_CONFIG["port"]
    database = POSTGRES_CONFIG["database"]
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)


def load_to_postgres(df: pd.DataFrame, table_name: str) -> None:
    engine = get_postgres_engine()
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"Cargados {len(df)} registros en la tabla {table_name}")
