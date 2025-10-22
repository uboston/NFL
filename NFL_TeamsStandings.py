from balldontlie import BalldontlieAPI
from dotenv import load_dotenv
import os
import pandas as pd
from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization
load_dotenv()

with open(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"), "rb") as key:
    p_key = serialization.load_pem_private_key(key.read(), password=None)
    private_key_bytes = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

connection_parameters = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "private_key": private_key_bytes,
    "authenticator": os.getenv("SNOWFLAKE_AUTHENTICATOR"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA")
}

session = Session.builder.configs(connection_parameters).create()

# -----------------------------------------------------------------

api_key = os.getenv("BALLDONTLIE_API_KEY")
api = BalldontlieAPI(api_key=api_key)


# 2010 - 2024 TEAM STANDINGS (ran once and saved in table)
year = 2024

while (year >= 2010):

    standings = api.nfl.standings.get(season=year)

    def to_dict(obj):
        if hasattr(obj, "__dict__"):
            result = {}
            for key, value in obj.__dict__.items():
                if isinstance(value, list):
                    result[key] = [to_dict(item) for item in value]
                else:
                    result[key] = to_dict(value)
            return result
        else:
            return obj

    data = [to_dict(item) for item in standings.data]
    if year == 2024:
        df = pd.json_normalize(data)

    else:
        df = pd.concat([df, pd.json_normalize(data)], ignore_index=True)
    year -= 1

# Convert specified columns to nullable integer type
cols_to_int = ["season", "win_streak", "points_for", "points_against", "playoff_seed", "point_differential", "wins", "losses", "ties", "team.id"]
for col in cols_to_int:
    df[col] = pd.to_numeric(df[col], errors="coerce")  # convert to float first
    if df[col].notna().all():  # if all valid numbers
        df[col] = df[col].astype("Int64")
        print(df[col].dtype)



# 2025 TEAM STANDINGS (run every friday, monday, and tuesday during the season, drop and replace 2025 season data every time)
standings2025 = api.nfl.standings.get(season=2025)

def to_dict(obj):
    if hasattr(obj, "__dict__"):
        result = {}
        for key, value in obj.__dict__.items():
            if isinstance(value, list):
                result[key] = [to_dict(item) for item in value]
            else:
                result[key] = to_dict(value)
        return result
    else:
        return obj

data = [to_dict(item) for item in standings2025.data]
df2 = pd.json_normalize(data)

# Convert specified columns to nullable integer type for 2025 standings dataframe
for col in cols_to_int:
    df2[col] = pd.to_numeric(df2[col], errors="coerce")  # convert to float first
    if df2[col].notna().all():  # if all valid numbers
        df2[col] = df2[col].astype("Int64")
        print(df2[col].dtype)

