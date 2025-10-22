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



api_key = os.getenv("BALLDONTLIE_API_KEY")
api = BalldontlieAPI(api_key=api_key)

# PLAYERS

cursor = None
all_players = []

while True:
    try:
        players = api.nfl.players.list(cursor=cursor, per_page=100)
    except Exception as e:
        print(f"Error fetching cursor {cursor}: {e}")
        break

    for p in players.data:
        # Safely handle None values by replacing with empty string
        all_players.append({
            "id": p.id,
            "first_name": p.first_name or "",
            "last_name": p.last_name or "",
            "position": p.position or "",
            "position_abbreviation": p.position_abbreviation or "",
            "height": p.height or "",
            "weight": p.weight or "",
            "jersey_number": p.jersey_number or "",
            "college": p.college or "",
            "experience": p.experience or "",
            "age": p.age or "",
            "team_id": p.team.id if p.team else ""
        })

    if not players.meta.next_cursor:
        break

    cursor = players.meta.next_cursor

# Flatten and normalize the data
df_players = pd.json_normalize(all_players)

# Convert numeric columns and fill non-numeric with empty string
numeric_cols = ['id', 'team_id', 'age']
for col in numeric_cols:
    df_players[col] = pd.to_numeric(df_players[col], errors='coerce')

for col in df_players.columns:
    if col not in numeric_cols:
        df_players[col] = df_players[col].fillna("")

print(df_players.head())
print(df_players.dtypes)

session.write_pandas(df_players, table_name="NFL_PLAYERS", auto_create_table=True, overwrite=True)