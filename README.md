# ADM ( Advance Data Management ) Project

+ Data Cleaning
+ NoSQL methods
+ Aggrigate Oriented DataBases
+ E-D Diagram
+ Document Base Database (MONGODB)
+ Column-Family Base Database (Casandra)
+ Graph Base Database (Neo4j)

Convert sqlite file into mongoDB database and write its Queries That i mentioned all of them in notebook with this code:

## Match Collection:
```python
import math
import pymongo
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["new_db_socer1"]

collection_match = db["match"]
for index, row in match.iterrows():
    leagues = league[league["id"] == row["league_id"]].iloc[0]
    countries = country[country["id"] == row["country_id"]].iloc[0]
    matches = {
        "id" : int(row["id"]),
        "season" : row["season"],
        "stage" : row["stage"],
        "date" : row["date"],
        "home_team": row["home_team_api_id"],
        "away_team": row["away_team_api_id"],
        "home_player": row["home_player"],
        "away_player": row["away_player"],
        "home_team_goal" : row["home_team_goal"],
        "away_team_goal" : row["away_team_goal"],
        "match_status" : row["match_status"],
        "league" : {
            "id": int(row["league_id"]),
            "name" : leagues["name"],
            "country": {
                "id": int(row["league_id"]),
                "name" : countries["name"],
            }
        },
     }
    collection_match.insert_one(matches)
```
##Teams Collection:

```python
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["new_db_socer1"]

collection_teams = db["teams"]
for index, row in team.iterrows():
    team_attr_df = team_attr[team_attr["team_api_id"] == row["team_api_id"]]
    
    team_attrs = []
    for _, ht_attr in team_attr_df.iterrows():
        team_attrs.append({
            "id": int(ht_attr["id"]),
            "buildUpPlaySpeedClass" : ht_attr["buildUpPlaySpeedClass"],
            "buildUpPlayDribblingClass" : ht_attr["buildUpPlayDribblingClass"],
            "buildUpPlayPassingClass" : ht_attr["buildUpPlayPassingClass"] ,
            "defencePressure" : int(ht_attr["defencePressure"]),
            "chanceCreationPassing" : int(ht_attr["chanceCreationPassing"]),
            "date" : ht_attr["date"]
        })
    
    teams = {
        "id" : int(row["id"]),
        "team_api_id" : int(row["team_api_id"]),
        "team_name" : row["team_long_name"],
        "team_short_name" : row["team_short_name"],
        "team_attributes" : team_attrs
    }
    collection_teams.insert_one(teams)
```

## Player Collection:

```python
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["new_db_socer1"]

collection_player = db["player"]
for index, row in player.iterrows():
    player_attr_df = player_attr[player_attr["player_api_id"] == row["player_api_id"]]
    
    player_attrs = []
    for _, ht_attr in player_attr_df.iterrows():
        player_attrs.append({
            "id": int(ht_attr["id"]),
            "player_api_id" : ht_attr["player_api_id"],
            "overall_rating" : ht_attr["overall_rating"],
            "potential" : ht_attr["potential"] ,
            "preferred_foot" : ht_attr["preferred_foot"],
            "penalties" : float(ht_attr["penalties"]),
        })
    players = {
        "id" : int(row["id"]),
        "player_name" : row["player_name"],
        "birthday":row["birthday"],
        "height":int(row["height"]),
        "weight":int(row["weight"]),
        "player_attribute": player_attrs
    }
    
    collection_player.insert_one(players)
```

MongoDB Query Sample:
```python
query1 = [
    {
        "$match": {
            "home_team_goal": {"$gte": 3},
            "away_team_goal": {"$lte": 5},
            "season": {"$in": ["2008/2009", "2010/2011"]},
            "league.country.name": "Italy"
        }
    },
    {
        "$project": {
            "_id": 0,
            "home_team_goal": 1,
            "away_team_goal": 1,
            "date": 1,
            "stage": 1,
            "league.name" : 1
        }
    }
]
results = collection_match.aggregate(query1)
for result in results:
    pprint(result)
```
<img src="/New Folder/First.drawio.png">
