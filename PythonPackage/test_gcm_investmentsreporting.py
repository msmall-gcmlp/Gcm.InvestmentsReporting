from src.gcm.InvestmentsReporting.Runners.investmentsreporting import (
    InvestmentsReportRunner,
)
import pandas as pd
import datetime as dt


class TestExcelio:
    def test_simple_print(self):
        data = {
            "Name": ["1", "2", "3", "4"],
            "Age": [20, 21, 19, 18],
        }
        df = pd.DataFrame(data)
        rd = dt.datetime(2021, 11, 30)
        file_name = "Test_Data"
        v = InvestmentsReportRunner().execute(
            rundate=rd,
            input_data=df,
            print_type="simple",
            location="C:/Temp/",
            file_name=file_name,
            save=False,
        )
        assert v is not None

    def test_write_dataframe_to_xl(self):
        data_1 = {
            "Name": ["1", "2", "3", "4"],
            "Age": [20, 21, 19, 18],
        }
        data_1 = pd.DataFrame(data_1)

        data_2 = {
            "Name": ["A", "B", "C", "D"],
            "Age": [20, 21, 19, 18],
        }
        data_2 = pd.DataFrame(data_2)

        input_data = {"Sheet1": {"C1": data_1, "Z1": data_2}}

        rd = dt.datetime(2021, 11, 30)
        file_name = "Test_Data"
        v = InvestmentsReportRunner().execute(
            rundate=rd,
            input_data=input_data,
            print_type="templated",
            template_name="PvmTopPositionsReport.xlsx",
            file_name=file_name,
            save=True,
        )
        assert v is not None
