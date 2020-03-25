import pandas as pd
import pytest
import miner as m
from datetime import date

test_match_id = 7828232

def test_lineups_v1():
    r = m.sofascore.SofaRequests()
    resp = r.parse_lineups_event(test_match_id)
    assert isinstance(resp, dict)
    teams = ['homeTeam', 'awayTeam']
    for team in teams:
        # Test teams are present
        assert team in resp
        team_resp = resp[team]
        # Test formation is present
        assert 'formation' in team_resp
        # Test lineup sorted is present
        assert 'lineupsSorted' in team_resp
        lineups = team_resp['lineupsSorted']
        assert len(lineups) > 0
        for lineup in lineups:
            assert 'player' in lineup
            assert 'id' in lineup['player']
            assert 'name' in lineup['player']
            assert 'positionName' in lineup
            assert 'positionNameshort' in lineup

def test_lineups_v2():
    r = m.sofascore.SofaRequests()
    resp = r.parse_lineups_event_visual(test_match_id)
    assert isinstance(resp, dict)
    teams = ['homeTeam', 'awayTeam']
    for team in teams:
        # Test teams are present
        assert team in resp
        team_resp = resp[team]
        # Test formation is present
        assert 'formation' in team_resp
        # Test lineup sorted is present
        assert 'lineupsSorted' in team_resp
        lineups = team_resp['lineupsSorted']
        assert len(lineups) > 0
        for lineup in lineups:
            assert 'player' in lineup
            assert 'id' in lineup['player']
            assert 'name' in lineup['player']
            assert 'positionName' in lineup
            assert 'positionNameshort' in lineup

def test_fetch_future_match():
    match_id = 7828232

    handler = m.sofascore.SofaHandler(config={'multithreading': False})
    result, _ = handler.fetch_matches(match_id)
    assert len(result.keys()) == 415

def test_fetch_past_date():
    start_date = date(2019, 5, 2)

    handler = m.sofascore.SofaHandler(config={'multithreading': False})
    result, _ = handler.fetch_dates(start=start_date)
    assert len(result.keys()) == 427