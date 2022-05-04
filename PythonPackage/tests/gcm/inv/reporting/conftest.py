import os
import json
import pytest


@pytest.fixture
def performance_quality_report_inputs():
    f = open(os.path.dirname(__file__) + "\\test_data\\performance_quality_report_inputs.json")
    return json.load(f)


@pytest.fixture
def performance_quality_report_inputs_all():
    f = open(os.path.dirname(__file__) + "\\test_data\\performance_quality_report_inputs_all.json")
    return json.load(f)
