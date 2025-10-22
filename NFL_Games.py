from balldontlie import BalldontlieAPI
from dotenv import load_dotenv
import os
import pandas as pd
import re
from datetime import datetime, timezone

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

# ---------------------------------------------------------------------------
api_key = os.getenv("BALLDONTLIE_API_KEY")
api = BalldontlieAPI(api_key=api_key)

def extract_full_name(team):
    # 1) SDK model object
    if hasattr(team, "full_name"):
        return team.full_name
    # 2) Dict (e.g., if you used json payload directly)
    if isinstance(team, dict) and "full_name" in team:
        return team["full_name"]
    # 3) Fallback: parse from string representation
    m = re.search(r"full_name='([^']+)'", str(team))
    return m.group(1) if m else None

# GAMES

# Get all games from 2025 season that have been completed
games_list = []
year = 2025
today = datetime.now(timezone.utc)

for week in range(1,19): 
    cursor = None
    while True:
        allGames = api.nfl.games.list(seasons=[year], weeks=[week], per_page=100, cursor=cursor)
        # Convert only games that have happened (status == Final)
        for game in allGames.data:
            game_date = datetime.fromisoformat(game.date.replace("Z", "+00:00"))
            if game_date <= today and str(game.status).lower() == "final":
                games_list.append(game.__dict__)

        # Handle pagination
        cursor = getattr(allGames.meta, "next_cursor", None)
        if not cursor:
            break
      
df = pd.json_normalize(games_list)

df["home_team"] = df["home_team"].apply(extract_full_name)
df["visitor_team"] = df["visitor_team"].apply(extract_full_name)

num_cols = df.select_dtypes(include=["float", "int"]).columns
df[num_cols] = df[num_cols].fillna(0)

print(f"✅ Total completed games: {len(df)}")
print(df.tail(5))
print(df.iloc[-2])