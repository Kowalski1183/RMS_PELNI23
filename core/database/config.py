import os
import urllib.parse
from dotenv import load_dotenv
from pathlib import Path

headers = {
    "Authorization": "Bearer eyJhbGciOiJIUzUxMiIsImtpZCI6IlRYbnA4YlR4UFNXVGVoLUtzXzJDcGdZYTNhZV9KWkhoYUNzTmdseFByUlkifQ.eyJhdWQiOiJodHRwczovL2FwaS1kZXYucGVsbmkuY28uaWQiLCJlbWFpbCI6ImFwaS1hY2Nlc3NAYXN5c3QuY29tIiwiZXhwIjoxODM5MTQxMjYyLCJpbnN0YW5zaSI6bnVsbCwiaXNzIjoiaHR0cDovL21zLWF1dGgtbWFuYWdlbWVudCIsImp0aSI6IjM4NzMwZDM1YjBjODdlZDUwNThmNDU1ZjJlNmE4YzIwMjg3OTY3YzA4ODA4MDZiZWY1ZmJjOGQxODJhYjBkZTMiLCJyb2xlcyI6WyJleHRlcm5hbCJdLCJzdWIiOiJhcGktYWNjZXNzQGFzeXN0LmNvbSIsInVzZXJOYW1lIjoiYXN5c3QifQ.2LM42aQ_Q_QeuAo5gkaOkalI4MjFQdrLIAlgX5yVuu1loJiE7a7uDsV2kqubnZiy5-SGV1k3q7uh7ZnvayfkXg"
}

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)


class Settings:
    POSTGRES_USER: str = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = urllib.parse.quote_plus(os.getenv("POSTGRES_PASSWORD"))
    POSTGRES_SERVER: str = os.getenv("POSTGRES_SERVER")
    POSTGRES_PORT: str = os.getenv("POSTGRES_PORT")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB")
    POSTGRES_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}"

settings = Settings()
