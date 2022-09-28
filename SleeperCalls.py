import requests
import pandas as pd
import json
from datetime import date
from os.path import exists

week = str(1)


def get_json_users():
    global path
    path = "https://api.sleeper.app/v1/league/" + leagueId + "/users"
    path_roster = "https://api.sleeper.app/v1/league/" + leagueId + "/rosters"
    users = requests.get(path).text
    roster = requests.get(path_roster).text
    jsonUsers = json.loads(users)
    jsonRoster = json.loads(roster)
    return jsonUsers, jsonRoster


def get_roster_id(userId, jsonRoster):
    for roster in jsonRoster:
        if roster["owner_id"] == userId:
            return roster["roster_id"]

    # This is for if the owner of the team is not the original owner
    for roster in jsonRoster:
        if roster["co_owners"]:
            for owner in roster["co_owners"]:
                if userId == owner:
                    return roster["roster_id"]


def transform_json_dataframe_users(jsonUsers, jsonRoster):
    userIds = []
    displayNames = []
    teamNames = []
    rosterIds = []
    for user in jsonUsers:
        userId = user["user_id"]
        userIds.append(userId)
        rosterIds.append(get_roster_id(userId, jsonRoster))
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
    path = "https://api.sleeper.app/v1/league/" + leagueId + "/matchups/" + week
    jsonMatchups = json.loads(requests.get(path).text)
    return jsonMatchups


def download_players(filename):
    path = "https://api.sleeper.app/v1/players/nfl"
    with open(filename, "w") as f:
        players = json.loads(requests.get(path).text)
        json.dump(players, f)


def get_json_players():
    today = date.today()
    today = today.strftime("%d_%m_%y")
    filename = "players_" + today + ".json"
    if not exists(filename):
        print("downloading players")
        download_players(filename)
    with open(filename, 'r') as j:
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


if __name__ == "__main__":
    with open("config") as config:
        leagueId = config.readline()

    jsonUsers, jsonRoster = get_json_users()
    dfUsers = transform_json_dataframe_users(jsonUsers, jsonRoster)

    jsonMatchups = get_json_matchups()
    dfMatchups = transform_json_dataframe_matchups(jsonMatchups)

    jsonPlayers = get_json_players()
    dfPlayers = transform_json_dataframe_players(jsonPlayers)

    dfUsersMatchups = pd.merge(dfUsers, dfMatchups, how="left", on="roster_id")
    dfScores = pd.merge(dfUsersMatchups, dfPlayers, how="left", on="player_id")
    dfScores.to_csv('out.csv', index=False)