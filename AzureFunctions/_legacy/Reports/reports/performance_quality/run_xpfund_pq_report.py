import json
import logging
import pandas as pd
import datetime as dt

from _legacy.Reports.reports.performance_quality.xpfund_pq_report import generate_xpfund_pq_report_data
from _legacy.Reports.reports.performance_quality.xpfund_highlow_pq_screen import XPfundHighLowPQScreen
from _legacy.core.ReportStructure.report_structure import (
    ReportingEntityTypes,
    ReportType,
    AggregateInterval,
    ReportVertical,
)
from _legacy.core.Runners.investmentsreporting import (
    InvestmentsReportRunner,
)
from _legacy.core.reporting_runner_base import (
    ReportingRunnerBase,
)
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs, DaoSource
from gcm.inv.scenario import Scenario
from functools import cached_property
from _legacy.Reports.reports.performance_quality.helper import PerformanceQualityHelper
from _legacy.Reports.reports.performance_quality.xpfund_highlow_data_analysis import _xpfund_data_to_highlow_df

class RunXPFundPqReport(ReportingRunnerBase):
    def __init__(self, runner, as_of_date):
        #super().__init__(runner=Scenario.get_attribute("runner"))
        super().__init__(runner=runner)
        self._as_of_date = as_of_date
        self._underlying_data_location = "raw/investmentsreporting/summarydata/xpfund_performance_quality"
        #self._helper = PerformanceQualityHelper()
        #self._dao = Scenario.get_attribute("dao")

    def generate_report(self, inv_group_ids=None, additional_ids=None,
                        custom_report_name=None, write_to_reporting_hub=False):
        #x=self._fund_inputs
        
        #fund_dimn = pd.read_json(self._fund_inputs["report_data"], orient="index")
        report_data = generate_xpfund_pq_report_data(runner=self._runner,
                                                     date=self._as_of_date,
                                                     inv_group_ids=inv_group_ids,
                                                     additional_ids=additional_ids)

        report_data.loc[report_data['InvestmentGroupName'] == 'D1 Capital', 'InvestmentGroupName'] = 'D1 - GIP'
        report_data.loc[report_data['InvestmentGroupName'] == 'D1 Liquid Class', 'InvestmentGroupName'] \
            = 'D1 - Publics Only'
        report_data_orig = report_data.drop('absolute_return_benchmark', axis=1)
        report_data_orig = report_data_orig.drop(('AbsoluteReturnBenchmarkExcessLag', '3Y'), axis=1)
        report_data_orig = report_data_orig.drop(('AbsoluteReturnBenchmarkExcessLag', 'ITD'), axis=1)
        input_data = {
            'as_of_date': pd.DataFrame({'date': [self._as_of_date]}),
            'report_data': report_data_orig
        }

        input_data_json = {
            'as_of_date': pd.DataFrame({'date': [self._as_of_date]}).to_json(orient="index"),
            'report_data': report_data.to_json(orient="index"),
        }

        print_areas = {'XPFUND_Performance_Quality': 'FL1:FU3'}

        date = dt.datetime.combine(self._as_of_date, dt.datetime.min.time())

        if inv_group_ids is None:
            report_name = "ARS Performance Quality - Firm x Portfolio Fund"
        else:
            report_name = custom_report_name

        data_to_write = json.dumps(input_data_json)
        as_of_date = self._as_of_date.strftime("%Y-%m-%d")
        write_params = AzureDataLakeDao.create_get_data_params(
            self._underlying_data_location,
            "_firm_x_portfolio_fund" + as_of_date + ".json",
            retry=False,
        )
        self._runner.execute(
            params=write_params,
            source=DaoSource.DataLake,
            operation=lambda dao, params: dao.post_data(params, data_to_write),
        )

        logging.info("JSON stored to DataLake")

        if write_to_reporting_hub:
            with Scenario(as_of_date=date).context():
                InvestmentsReportRunner().execute(
                    data=input_data,
                    template="XPFUND_PerformanceQuality_Template.xlsx",
                    save=True,
                    runner=self._runner,
                    entity_type=ReportingEntityTypes.cross_entity,
                    entity_name='FIRM',
                    entity_display_name='FIRM',
                    report_name=report_name,
                    report_type=ReportType.Performance,
                    report_vertical=ReportVertical.ARS,
                    report_frequency="Monthly",
                    aggregate_intervals=AggregateInterval.MTD,
                    print_areas=print_areas
                )
        else:
            with Scenario(as_of_date=date).context():
                InvestmentsReportRunner().execute(
                    data=input_data,
                    template="XPFUND_PerformanceQuality_Template.xlsx",
                    save=True,
                    runner=self._runner,
                    entity_type=ReportingEntityTypes.cross_entity,
                    entity_name='FIRM',
                    entity_display_name='FIRM',
                    report_name=report_name,
                    report_type=ReportType.Performance,
                    report_vertical=ReportVertical.ARS,
                    report_frequency="Monthly",
                    aggregate_intervals=AggregateInterval.MTD,
                    print_areas=print_areas,
                    output_dir="cleansed/investmentsreporting/printedexcels/",
                    report_output_source=DaoSource.DataLake
                )
        #self.generate_highlow_report

    # def _download_inputs(self, runner, dl_location, file_path) -> dict:
    #     try:
    #         read_params = AzureDataLakeDao.create_get_data_params(
    #             dl_location,
    #             file_path,
    #             retry=False,
    #         )
    #         file = runner.execute(
    #             params=read_params,
    #             source=DaoSource.DataLake,
    #             operation=lambda dao, params: dao.get_data(read_params),
    #         )
    #         inputs = json.loads(file.content)
    #     except:
    #         inputs = None
    #     return inputs

    # @cached_property
    # def _xpfund_pq_report(self):       
    #     as_of_date = self._as_of_date.strftime("%Y-%m-%d")
    #     inputs = PerformanceQualityHelper().download_inputs(
    #         location="raw/investmentsreporting/summarydata/xpfund_performance_quality", 
    #         file_path ="_firm_x_portfolio_fund" + as_of_date + ".json"
    #     )
    #     return inputs

    # def _get_xpfund_pq_report(self, runner, as_of_date):
    #     inputs = self._xpfund_pq_report
    #     xpfund_inputs = pd.read_json(inputs['report_data'], orient='index')

    #     return xpfund_inputs
    
    
    def generate_highlow_report(self):
        high_low_report = XPfundHighLowPQScreen(
            runner=self._runner,
            as_of_date=self._as_of_date
        )
        return high_low_report.execute()

    def run(self, inv_group_ids=None, **kwargs):
        self.generate_report(inv_group_ids=inv_group_ids,
                             additional_ids=kwargs.get('additional_ids'),
                             custom_report_name=kwargs.get('custom_report_name'),
                             write_to_reporting_hub=kwargs.get('write_to_reporting_hub', False))
        #self.generate_highlow_report()
        #self.generate_xpfund_high_pq_screen_data
        return "Complete"


if __name__ == "__main__":
    dao_runner = DaoRunner(
            container_lambda=lambda b, i: b.config.from_dict(i),
            config_params={
                DaoRunnerConfigArgs.dao_global_envs.name: {
                    DaoSource.DataLake.name: {
                        "Environment": "dev",
                        "Subscription": "nonprd",
                    },
                    DaoSource.PubDwh.name: {
                        "Environment": "prd",
                        "Subscription": "prd",
                    },
                    DaoSource.InvestmentsDwh.name: {
                        "Environment": "prd",
                        "Subscription": "prd",
                    },
                    DaoSource.DataLake_Blob.name: {
                        "Environment": "prd",
                        "Subscription": "prd",
                    },
                    DaoSource.ReportingStorage.name: {
                        "Environment": "dev",
                        "Subscription": "nonprd",
                    },
                }
            })

    date = dt.date(2022, 4, 30)
    with Scenario(as_of_date=date).context():

        report_runner = RunXPFundPqReport(runner=dao_runner, as_of_date=date)

        # firmwide report. # add in D1 Liquid Class which isn't an EMM
        report_runner.execute(additional_ids=[23447])

        #report_runner.generate_highlow_report()

        # esg report
        # custom_report_name = "ARS Performance Quality - ESG x Portfolio Fund"
        # report.execute(inv_group_ids=[19717, 20292, 20319, 31378, 89745, 43058, 51810, 86478, 87478, 89809],
        #                custom_report_name="ARS Performance Quality - ESG x Portfolio Fund",
        #                write_to_reporting_hub=False)
