from src.gcm.InvestmentsReporting.Runners.investmentsreporting import (
    InvestemtnsReportingRunner,
)
import pandas as pd
import datetime as dt


def test_simple_print():
    data = {
        "Name": ["1", "2", "3", "4"],
        "Age": [20, 21, 19, 18],
    }
    df = pd.DataFrame(data)
    asofdate = dt.datetime(2021, 11, 30)
    file_name = "Test_Data"
    v = InvestemtnsReportingRunner().execute(
        asofdate=asofdate,
        input_data=df,
        print_type="simple",
        location="C:/Temp/",
        file_name=file_name,
    )
    assert v is not None
