# Run in root directory: pytest lib/tests/api_test.py -v

import pytest
from fastapi.testclient import TestClient
from lib.api import app

client = TestClient(app)

#@pytest.mark.skip(reason="Does not need testing ATM.")
def testSearch(): # Test case for all search functions (username, prefix, guild name, uuids)
    testPrefixs = ["SEQ", "PROF", "Nia", "SDU", "bean"]
    testNames = ["Aequitas", "The Broken Gasmask", "HackForums", "Chiefs Of Corkus", "Jasmine Dragon"]
    testUUIDs = ["c39a4636-1646-4b5a-88cc-a9a28b915c6a", "749c4943-0335-459c-85b3-43c0113cde50", "15192250-9f7b-49b0-86d9-4146c940b775", "ddbdac33-9f3c-4e25-af92-2071d127a246", "b250f587-ab5e-48cd-bf90-71e65d6dc9e7"]
    testUsernames = ["BadPingHere", "RealAlex", "LegitNube", "Shisouhan", "Csm1tty"]
    
    
    # /api/search/prefix/{prefix}
    for prefix in testPrefixs:
        response = client.get(f"/api/search/prefix/{prefix}")
        assert response.status_code == 200, f"Prefix failed testSearch: {prefix}"
        jsonData = response.json()
        # Ensure prefix is correct, if so likely everything else is correct.
        assert jsonData["prefix"].lower().startswith(prefix.lower()), (f"Expected prefix '{prefix}', got '{jsonData['prefix']}'")

    for name in testNames:
        response = client.get(f"/api/search/name/{name}")
        assert response.status_code == 200, f"Name failed testSearch: {name}"
        jsonData = response.json()
        # Ensure name is correct, if so likely everything else is correct.
        assert jsonData["name"].lower().startswith(name.lower()), (f"Expected name '{name}', got '{jsonData['name']}'")
    
    for uuid in testUUIDs:
        response = client.get(f"/api/search/uuid/{uuid}")
        assert response.status_code == 200, f"UUID failed testSearch: {uuid}"
        jsonData = response.json()
        # Ensure name is correct, if so likely everything else is correct.
        assert jsonData["uuid"].lower().startswith(uuid.lower()), (f"Expected UUID '{uuid}', got '{jsonData['uuid']}'")

    for username in testUsernames:
        response = client.get(f"/api/search/username/{username}")
        assert response.status_code == 200, f"Username failed testSearch: {username}"
        jsonData = response.json()
        # Ensure name is correct, if so likely everything else is correct.
        assert jsonData["username"].lower().startswith(username.lower()), (f"Expected username '{username}', got '{jsonData['username']}'")

#@pytest.mark.skip(reason="Does not need testing ATM.")
def testGraids(): # Tests /eligible and /completions
    expected = {"SEQ", "Aeq", "TAq", "AVO", "ANO"} # These guys should ALWAYS be in there


    response = client.get(f"/api/graid/eligible")
    assert response.status_code == 200, f"Eligible route failed."
    jsonData = response.json()
    guilds = set(jsonData.get("guilds", []))
    missing = expected - guilds
    assert not missing, f"Missing expected guild prefixes: {missing}"

    response = client.get(f"/api/graid/completions")
    assert response.status_code == 200, f"Completions route failed."
    jsonData = response.json()
    guilds = set(jsonData.keys())
    missing = expected - guilds
    assert not missing, f"Missing expected guild prefixes: {missing}"
    for g in expected:
        assert g in jsonData, f"{g} missing entirely in completions."
        assert jsonData[g] is not None, f"{g} has None data in completions."
        assert isinstance(jsonData[g], list), f"{g} data must be a list."
        
#@pytest.mark.skip(reason="Does not need testing ATM.")
def testMaps(): # Tests heatmap and normal map
    testHeatmap = ["Season 26", "Everything", "Last 7 Days"]
    
    response = client.get(f"/api/map/current")
    assert response.status_code == 200, f"Current map route failed."
    assert isinstance(response.content, bytes), "Current map response is not raw bytes."
    
    for timeframe in testHeatmap:
        response = client.get(f"/api/map/heatmap?timeframe={timeframe}")
        assert response.status_code == 200, f"Timeframe failed testMaps: {timeframe}."
        assert isinstance(response.content, bytes), "Current map response is not raw bytes."

#@pytest.mark.skip(reason="Does not need testing ATM.")
def testLeaderboard(): # Tests all leaderboards
    uuids = ["ee860b7c-9a1d-49cf-9f19-ab673ba0f23b", "cef53bae-dc42-46aa-8ccf-1b10d282b420"] # SEQ, ANO
    timeframes = ["Last 24 Hours", "Last 14 Days", "Everything"]
    leaderboardTypes = [ # All current leaderboard types
        "guildLeaderboardOnlineMembers", 
        "guildLeaderboardTotalMembers",
        "guildLeaderboardWars",
        "guildLeaderboardXP",
        "playerLeaderboardRaids",
        "playerLeaderboardDungeons",
        "playerLeaderboardPVPKills",
        "playerLeaderboardTotalLevel",
        "playerLeaderboardPlaytime",
        "guildLeaderboardXPButGuildSpecific",
        "guildLeaderboardOnlineButGuildSpecific",
        "guildLeaderboardWarsButGuildSpecific",
        "guildLeaderboardGraids",
        "guildLeaderboardGraidsButGuildSpecific",
        "playerLeaderboardGraids",
    ]
    for uuid in uuids:
        for timeframe in timeframes:
            for leaderboardType in leaderboardTypes:
                response = client.get(f"/api/leaderboard/{leaderboardType}?uuid={uuid}&timeframe={timeframe}")
                assert response.status_code == 200, f"Timeframe & UUID & leaderboardType failed testLeaderboard: timeframs is {timeframe}, uuid is {uuid}, leaderboardType is {leaderboardType}."
                jsonData = response.json()
                assert isinstance(jsonData, list), f"Timeframe & UUID & leaderboardType failed testLeaderboard, response is not a list: timeframs is {timeframe}, uuid is {uuid}, leaderboardType is {leaderboardType}."
                #assert len(jsonData) > 0, f"Timeframe & UUID & leaderboardType failed testLeaderboard, list is empty: timeframs is {timeframe}, uuid is {uuid}, leaderboardType is {leaderboardType}."

#@pytest.mark.skip(reason="Does not need testing ATM.")
def testActivity(): # Tests all activity endpoints
    uuidANDPrefix = [["ee860b7c-9a1d-49cf-9f19-ab673ba0f23b", "SEQ"], ["cef53bae-dc42-46aa-8ccf-1b10d282b420", "ANO"]]
    uuidANDUsername = [["f129b394-0e59-4681-b051-b2c102f12dd6", "Shisouhan"], ["aa7402cc-bf1c-4aed-838b-fd8897d38836", "WoodCreature"]]
    timeframes = ["Last 14 Days", "Last 30 Days"]
    guildActivityTypes = [
        "guildActivityXP", 
        "guildActivityTerritories",
        "guildActivityWars",
        "guildActivityOnlineMembers",
        "guildActivityTotalMembers",
        "guildActivityGraids",
    ]
    playerActivityTypes = [
        "playerActivityPlaytime",
        "playerActivityContributions",
        "playerActivityDungeons",
        "playerActivityTotalDungeons",
        "playerActivityRaids",
        "playerActivityTotalRaids",
        "playerActivityMobsKilled",
        "playerActivityWars",
        "playerActivityGraids",
    ]
    # TODO: if we use a very narrow timeframe (Last 24 hours) for something like players and they havent logged in within the past 24 hours, we get a failed test-case when it returns 500.
    for uuid, prefix in uuidANDPrefix:
        for timeframe in timeframes:
            for activityType in guildActivityTypes:
                response = client.get(f"/api/activity/{activityType}?uuid={uuid}&name={prefix}&timeframe={timeframe}&theme=light")
                assert response.status_code == 200, f"Activitytype/uuid/name/timeframe/theme failed testActivity: timeframe is {timeframe}, uuid is {uuid}, activityType is {activityType}, name is {prefix}"
                jsonData = response.json()
                assert "image" in jsonData, f"Activitytype/uuid/name/timeframe/theme failed testActivity, no image given: timeframe is {timeframe}, uuid is {uuid}, activityType is {activityType}, name is {prefix}"

    for uuid, username in uuidANDUsername:
        for timeframe in timeframes:
            for activityType in playerActivityTypes:
                response = client.get(f"/api/activity/{activityType}?uuid={uuid}&name={username}&timeframe={timeframe}&theme=light")
                assert response.status_code == 200, f"Activitytype/uuid/name/timeframe/theme failed testActivity: timeframe is {timeframe}, uuid is {uuid}, activityType is {activityType}, name is {username}"
                jsonData = response.json()
                assert "image" in jsonData, f"Activitytype/uuid/name/timeframe/theme failed testActivity, no image given: timeframe is {timeframe}, uuid is {uuid}, activityType is {activityType}, name is {username}"
