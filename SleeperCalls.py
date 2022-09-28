import requests
import pandas as pd
import json
from datetime import date
import os.path
from pathlib import Path, PurePath

global league_id
global week


def get_json_users():
    path = "https://api.sleeper.app/v1/league/" + league_id + "/users"
    path_roster = "https://api.sleeper.app/v1/league/" + league_id + "/rosters"
    users = requests.get(path).text
    roster = requests.get(path_roster).text
    json_users = json.loads(users)
    json_roster = json.loads(roster)
    return json_users, json_roster


def get_roster_id(userId, json_roster):
    for roster in json_roster:
        if roster["owner_id"] == userId:
            return roster["roster_id"]

    # This is for if the owner of the team is not the original owner
    for roster in json_roster:
        if roster["co_owners"]:
            for owner in roster["co_owners"]:
                if userId == owner:
                    return roster["roster_id"]


def transform_json_dataframe_users(json_users, json_roster):
    userIds = []
    displayNames = []
    teamNames = []
    rosterIds = []
    for user in json_users:
        userId = user["user_id"]
        userIds.append(userId)
        rosterIds.append(get_roster_id(userId, json_roster))
        displayNames.append(user["display_name"])
        try:
            teamNames.append(user["metadata"]["team_name"])
        except:
            teamNames.append(user["display_name"])
    users = {
        "user_id": userIds,
        "roster_id": rosterIds,
        "display_name": displayNames,
        "team_name": teamNames
    }

    return pd.DataFrame(users)


def transform_json_dataframe_matchups(jsonMatchups):
    rosterIds = []
    players = []
    points = []
    for matchup in jsonMatchups:
        rosterId = matchup["roster_id"]
        starters = matchup["starters"]
        player_points = matchup["players_points"]
        starter_points = {player: points for player, points in player_points.items() if player in starters}
        temp_players = starter_points.keys()
        temp_points = starter_points.values()
        players.extend(temp_players)
        points.extend(temp_points)
        rosterIds.extend([rosterId] * len(starters))

    matchups = {
        "roster_id": rosterIds,
        "player_id": players,
        "points": points
    }
    return pd.DataFrame(matchups)


def get_json_matchups():
    path = "https://api.sleeper.app/v1/league/" + league_id + "/matchups/" + week
    jsonMatchups = json.loads(requests.get(path).text)
    return jsonMatchups


def download_players(filename):
    path = "https://api.sleeper.app/v1/players/nfl"
    with open(filename, "w") as f:
        players = json.loads(requests.get(path).text)
        json.dump(players, f)


def get_json_players():
    file_players = PurePath("players_", get_today(), ".json")
    if not os.path.exists(file_players):
        print("downloading players")
        download_players(file_players)
    with open(file_players, 'r') as j:
        players = json.loads(j.read())
    return players


def transform_json_dataframe_players(jsonPlayers):
    player_names = []
    positions = []
    player_ids = []
    team = []
    for player in jsonPlayers.values():
        try:
            player_names.append(player["full_name"])
        except KeyError:
            continue
        positions.append(player["position"])
        player_ids.append(player["player_id"])
        team.append(player["team"])
    players = {
        "player_id": player_ids,
        "name": player_names,
        "position": positions,
        "team": team
    }
    return pd.DataFrame(players)


def configure_properties():
    global league_id, week
    with open("config") as config:
        league_id = config.readline().strip()
        week = config.readline().strip()


def merge_dataframes(df_users, df_matchups, df_players):
    df_users_matchups = pd.merge(df_users, df_matchups, how="left", on="roster_id")
    return pd.merge(df_users_matchups, df_players, how="left", on="player_id")


def get_today():
    today = date.today()
    today = today.strftime('%d_%m_%y')


if __name__ == "__main__":
    configure_properties()

    json_users, json_roster = get_json_users()
    df_users = transform_json_dataframe_users(json_users, json_roster)

    jsonMatchups = get_json_matchups()
    df_matchups = transform_json_dataframe_matchups(jsonMatchups)

    jsonPlayers = get_json_players()
    df_players = transform_json_dataframe_players(jsonPlayers)

    df_scores = merge_dataframes(df_users, df_matchups, df_players)

    output = PurePath("out_", get_today(), ".csv")

    df_scores.to_csv(output, index=False)
