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
# -------------------------------------
api_key = os.getenv("BALLDONTLIE_API_KEY")
api = BalldontlieAPI(api_key=api_key)


# PLAYERS

# Get all injuries this season with pagination
cursor = None
injury_data = []

while True:
    injuries = api.nfl.injuries.list(per_page=100, cursor=cursor)
    
    for injury in injuries.data:
        injury_data.append({
            "player_id": injury.player.id,
            "status": injury.status or "",
            "comment": injury.comment or "",
            "date": injury.date or ""
        })
    
    if not injuries.meta.next_cursor:
        break

    cursor = injuries.meta.next_cursor

df_injuries = pd.DataFrame(injury_data)
print(df_injuries.tail(25))
