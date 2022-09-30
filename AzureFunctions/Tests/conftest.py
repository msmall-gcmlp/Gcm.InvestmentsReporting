import os
import json
import pytest


@pytest.fixture
def skye_fund_inputs():
    f = open(os.path.dirname(__file__) + "\\test_data\\Skye_fund_inputs_2022-03-31.json")
    return json.load(f)


@pytest.fixture
def skye_primary_peer_inputs():
    f = open(os.path.dirname(__file__) + "\\test_data\\GCM TMT_peer_inputs_2022-03-31.json")
    return json.load(f)


@pytest.fixture
def skye_secondary_peer_inputs():
    f = open(os.path.dirname(__file__) + "\\test_data\\GCM Equities_peer_inputs_2022-03-31.json")
    return json.load(f)


@pytest.fixture
def skye_eh_inputs():
    f = open(os.path.dirname(__file__) + "\\test_data\\EHI100 LongShort Equity_eurekahedge_inputs_2022-03-31.json")
    return json.load(f)


@pytest.fixture
def skye_eh200_inputs():
    file = "\\test_data\\Eurekahedge Institutional 200_eurekahedge_inputs_2022-03-31.json"
    f = open(os.path.dirname(__file__) + file)
    return json.load(f)


@pytest.fixture
def market_factor_inputs():
    file = "\\test_data\\market_factor_returns_2022-03-31.json"
    f = open(os.path.dirname(__file__) + file)
    return json.load(f)
