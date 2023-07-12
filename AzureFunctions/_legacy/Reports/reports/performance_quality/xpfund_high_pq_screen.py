import datetime as dt
import json
import pandas as pd
import numpy as np
from gcm.Dao.DaoRunner import DaoRunnerConfigArgs, DaoRunner
from gcm.Dao.DaoSources import DaoSource
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from gcm.inv.scenario import Scenario
from gcm.inv.dataprovider.investment_group import InvestmentGroup
#from gcm.inv.models.highlow_pq_screen.highlow_xpfund_firmwide import _xpfund_data_to_highlow_df
from _legacy.Reports.reports.performance_quality.tmp import _3y_arb_xs_analysis,_3y_arb_xs_emm_percentiles
from _legacy.Reports.reports.performance_quality.tmp import _xpfund_data_to_highlow_df,_5y_arb_xs_emm_percentiles, _clean_firmwide_xpfund_data
from functools import partial, cached_property
from _legacy.Reports.reports.performance_quality.helper import PerformanceQualityHelper
from _legacy.core.reporting_runner_base import (
    ReportingRunnerBase,
)
from _legacy.core.ReportStructure.report_structure import (
    ReportingEntityTypes,
    ReportType,
    AggregateInterval,
    ReportVertical,
)
from _legacy.core.Runners.investmentsreporting import (
    InvestmentsReportRunner,
)

class XPfundHighPQScreen(ReportingRunnerBase):
    def __init__(self, runner, as_of_date):
        super().__init__(runner=runner)
        self._as_of_date = as_of_date
    
    def _download_inputs(self, runner, dl_location, file_path) -> dict:
        try:
            read_params = AzureDataLakeDao.create_get_data_params(
                dl_location,
                file_path,
                retry=False,
            )
            file = runner.execute(
                params=read_params,
                source=DaoSource.DataLake,
                operation=lambda dao, params: dao.get_data(read_params),
            )
            inputs = json.loads(file.content)
        except:
            inputs = None
        return inputs
    
    @cached_property
    def _xpfund_pq_report(self):       
        as_of_date = self._as_of_date.strftime("%Y-%m-%d")
        #file = self._fund_name.replace("/", "") + "_fund_inputs_" + as_of_date + ".json"
        inputs = PerformanceQualityHelper().download_inputs(
            location="raw/investmentsreporting/summarydata/xpfund_performance_quality", 
            file_path="_firm_x_portfolio_fund"+as_of_date+".json"
            )
        return inputs
        
    
    def _get_xpfund_pq_report(self, runner, as_of_date):
        inputs=self._xpfund_pq_report
    #file1 = "raw/investmentsreporting/summarydata/xpfund_performance_quality"
    #inputs = _download_inputs(runner=runner,dl_location="raw/investmentsreporting/summarydata/xpfund_performance_quality", file_path="_firm_x_portfolio_fund2023-03-31.json")
        x=pd.read_json(inputs['report_data'], orient='index')
        m=x[["InvestmentGroupName","('AbsoluteReturnBenchmarkExcess', '3Y')"]]
        #q=x.to_csv(r'C:\Code\filex1.csv')
        return x
    
    
    #send self._xpfund_pq_report to inv models to get the following new columns and also new df of high and low
    #def:  fill nan in copy of self._xpfund_pq_report["('AbsoluteReturnBenchmarkExcess', '3Y')"] with self._xpfund_pq_report["('AbsoluteReturnBenchmarkExcess', 'ITD')"]
    # sort by copy of 3y
    #new col: 3y percentile wrt EMM --> make col list of 3y copy --> %ile wrt list in new entry
    #new col: 5y percentile wrt EMM --> make col list of 
        # self._xpfund_pq_report["('AbsoluteReturnBenchmarkExcess', '5Y')"] --> %ile wrt list in new entry
    #high = 75 or above -->keep tail of df
    #low = 25 or below --> keep tail of df
    
    def generate_xpfund_high_pq_screen_data(self):
        with Scenario(dao=self._runner, as_of_date=self._as_of_date).context():
            date_q_minus_1 = pd.to_datetime(self._as_of_date - pd.tseries.offsets.QuarterEnd(1)).date()
            firm_xpfund_report_data = self._get_xpfund_pq_report(runner=self._runner, as_of_date=date_q_minus_1)
            firm_xpfund_highlow_df=firm_xpfund_report_data.copy()
            firm_xpfund_highlow_df=_xpfund_data_to_highlow_df(firm_xpfund_highlow_df, self._as_of_date)
            
        report_name="ARS Performance Quality - Firmwide High Performance Screen"
        high_rep_data= {
            "high_perf_summary": firm_xpfund_highlow_df[0],
            "high_perf_data": firm_xpfund_highlow_df[1],
            "low_perf_summary": firm_xpfund_highlow_df[2],
            "low_perf_data": firm_xpfund_highlow_df[3],
        }
        with Scenario(as_of_date=date).context():
            InvestmentsReportRunner().execute(
                    data=high_rep_data,
                    template="highlow_pq_1.xlsx",
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
                    # print_areas=print_areas,
                    # output_dir="cleansed/investmentsreporting/printedexcels/",
                    #report_output_source=DaoSource.DataLake
            )
        
        
        return firm_xpfund_highlow_df
    
    def run(self, **kwargs):
        try:
            self.generate_xpfund_high_pq_screen_data()
            return f"Complete"
        except Exception as e:
            raise RuntimeError(f"Failed") from e

if __name__ == "__main__":
    dao_runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.DataLake.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.InvestmentsDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.PubDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.ReportingStorage.name: {
                    "Environment": "dev",
                    "Subscription": "nonprd",
                },
            }
        },
    )
    date = dt.date(2023, 4, 30)
    inv_group_ids = None
    with Scenario(as_of_date=date).context():
        report=XPfundHighPQScreen(runner=dao_runner, as_of_date=date)
        report.execute()