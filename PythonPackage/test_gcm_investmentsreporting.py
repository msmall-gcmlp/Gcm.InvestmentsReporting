from src.gcm.InvestmentsReporting.Runners.investmentsreporting import (
    InvestmentsReportRunner,
)
import pandas as pd
import datetime as dt
from gcm.Scenario.scenario import Scenario
from gcm.Dao.DaoRunner import DaoRunner


class TestExcelio:
    def test_write_dataframe_to_xl(self):
        my_named_range = {
            "Name": ["1", "2", "3", "4"],
            "Age": [20, 21, 19, 18],
        }
        my_named_range = pd.DataFrame(my_named_range)

        my_second_range = {
            "Name": ["A", "B", "C", "D"],
            "Age": [20, 21, 19, 18],
        }
        my_second_range = pd.DataFrame(my_second_range)
        runner = DaoRunner()
        # TODO: reflect on variable names
        input_data = {
            "my_named_range": my_named_range,
            "my_second_range": my_second_range,
        }
        with Scenario(asofdate=dt.datetime(2021, 11, 30)).context():
            report_name = "Test_Data"
            InvestmentsReportRunner().execute(
                data=input_data,
                template="named_range_print_test.xlsx",
                save=True,
                report_name=report_name,
                runner=runner,
            )
