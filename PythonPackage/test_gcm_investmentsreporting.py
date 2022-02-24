from src.gcm.InvestmentsReporting.Runners.investmentsreporting import (
    InvestmentsReportRunner,
    
)
import pandas as pd
import datetime as dt
from gcm.Scenario.scenario import Scenario
from gcm.Dao.DaoRunner import DaoRunner
from src.gcm.InvestmentsReporting.ReportStructure.report_structure import (
    ReportingEntityTypes,
)
from gcm.Dao.DaoSources import DaoSource
from gcm.Dao.DaoRunner import DaoRunnerConfigArgs


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
        config_params = {
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.InvestmentsDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.PubDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
            }
        }
        runner = DaoRunner(
            container_lambda=lambda b, i: b.config.from_dict(i),
            config_params=config_params,
        )
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
                entity_name="EOFMF",
                entity_display_name="EOF",
                entity_type=ReportingEntityTypes.portfolio,
                entity_source=DaoSource.PubDwh,
            )

    def test_PDF(self):
        pdf_name = '202109_PFUND_ARS_TearSheet_Aspex.pdf'
        test_loc = "raw/test/rqstest"
        location = f"{test_loc}/rqstest/Reports To Upload/"
        with Scenario(asofdate=dt.datetime(2021, 11, 30)).context():
            report_name = "TearSheet"
            config_params = {
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.InvestmentsDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.PubDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                }
            }
            runner = DaoRunner(
                container_lambda=lambda b, i: b.config.from_dict(i),
                config_params=config_params,
            )
            InvestmentsReportRunner().execute(
                data=None,
                raw_pdf_name=pdf_name,
                raw_pdf_location=location,
                save=True,
                report_name=report_name,
                runner=runner,
                entity_name="Aspex Global",
                entity_display_name="Aspex",
                entity_type=ReportingEntityTypes.manager_fund,
                entity_source=DaoSource.PubDwh,
            )
            print("Done")

