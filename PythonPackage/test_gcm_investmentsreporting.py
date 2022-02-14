from src.gcm.InvestmentsReporting.Runners.investmentsreporting import (
    InvestmentsReportRunner,
)
import pandas as pd
import datetime as dt
from gcm.Scenario.scenario import Scenario
from gcm.Dao.DaoRunner import DaoRunner


class TestExcelio:
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
        runner = DaoRunner()
        input_data = {"my_named_range": data_1, "my_second_range": data_2}
        with Scenario(asofdate=dt.datetime(2021, 11, 30)).context():
            file_name = "Test_Data"
            v = InvestmentsReportRunner().execute(
                input_data=input_data,
                print_type="templated",
                template_name="named_range_print_test.xlsx",
                save=True,
                file_name=file_name,
                runner=runner,
            )
