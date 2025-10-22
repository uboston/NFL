import requests, json, time
import pandas as pd

# Create main dataframe
df = pd.DataFrame(columns= ['id', 'firstname', 'lastname', 'birthdate', 'height', 'weight', 'college', 'country', 'position(s)'])
df.set_index('id', inplace=True) 

# Loop through each team
for j in range (1,41):
  url = f"https://v2.nba.api-sports.io/players?team={j}&season=2023"
  payload={}
  headers = {
    'x-rapidapi-key': '{api-key}',
    'x-rapidapi-host': 'v1.american-football.api-sports.io'
  }

  # Request data from the API
  res = requests.get(url, headers=headers, data=payload)
  res_json = json.loads(res.text)

  # Create a new dataframe for each team
  newdf = pd.DataFrame(columns= ['id', 'firstname', 'lastname', 'birthdate', 'height', 'weight', 'college', 'country', 'position(s)'])
  newdf.set_index('id', inplace=True)

  # Loop through each player in the team
  for i in range(len(res_json['response'])):
      player = res_json['response'][i]

      id = player['id']
      fName = player['firstname']
      lName = player['lastname']
      birth = player['birth']['date']
      if (player['height']['feets'] != None) and (player['height']['inches'] != None):
        height = (f"{player['height']['feets']} ft {player['height']['inches']} in")
      elif (player['height']['feets'] != None) and (player['height']['inches'] == None):
        height = (f"{player['height']['feets']} ft")
      else:
        height = player['height']['feets']
      
      if player['weight']['pounds'] != None:
        weight = (f"{player['weight']['pounds']} lbs")
      else:
        weight = player['weight']['pounds']
      college = player['college']
      country = player['birth']['country']
      position = player['leagues']['standard']['pos']

      if j == 1:
        df.loc[id] = [fName, lName, birth, height, weight, college, country, position]
      else:
        newdf.loc[id] = [fName, lName, birth, height, weight, college, country, position]
        df = pd.concat([df, newdf], ignore_index=True)
  
  # Respect moment for 10 seconds
  time.sleep(3)


df

