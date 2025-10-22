# UNUSED FILE

from balldontlie import BalldontlieAPI
from dotenv import load_dotenv
import os
load_dotenv()


api_key = os.getenv("BALLDONTLIE_API_KEY")
api = BalldontlieAPI(api_key=api_key)
teams = api.nfl.teams.list()
teamIDs = []
playerIDs = []

# DONT THINK I NEED THIS ONE

#TEAMS

# Grab all team IDs and print their info
for team in teams.data:
    teamIDs.append(team.id)
#     print(f"ID: {team.id}, Team: {team.full_name} are in the {team.conference} {team.division}")
#   print(IDs)


# Grab specific teams by their IDs
for i in teamIDs:
    grabbedTeam = (api.nfl.teams.get(i))
    print(grabbedTeam)


# TEAM STANDINGS
standings = api.nfl.standings.get(season=2024)



