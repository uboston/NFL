from balldontlie import BalldontlieAPI
from dotenv import load_dotenv
import os
load_dotenv()

api_key = os.getenv("BALLDONTLIE_API_KEY")
api = BalldontlieAPI(api_key=api_key)
teams = api.nfl.teams.list()
teamIDs = []
playerIDs = []



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




# PLAYERS

# Grab all players
players = api.nfl.players.list()
for player in players.data:
    print(f"ID: {player.id}, Name: {player.first_name} {player.last_name}, Team ID: {player.team.id}, Position: {player.position_abbreviation}")

# Grab specific player by their ID
player = api.nfl.players.get({id})

# Get injuried players
injuries = api.nfl.injuries.list()

# Get all active players
activePlayers = api.nfl.players.list_active()



# GAMES

# Get all games
allGames = api.nfl.games.list()

# Specific game by game ID
specificGame = api.nfl.games.get({game_id})



# STATS

# Get all stats (use players_ids, games_ids, seassons as filters)
allStats = api.nfl.stats.list()

# Season stats
seasonStats = api.nfl.season_stats.list(season=2024)

# TEAM STANDINGS
standings = api.nfl.standings.get(season=2024)



