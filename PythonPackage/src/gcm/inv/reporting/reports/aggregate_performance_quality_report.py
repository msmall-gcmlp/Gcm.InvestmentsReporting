import functools
import json
import pandas as pd
import datetime as dt
from gcm.Dao.DaoSources import DaoSource
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from .reporting_runner_base import ReportingRunnerBase
from gcm.inv.dataprovider.portfolio import Portfolio
from gcm.inv.reporting.core.ReportStructure.report_structure import ReportingEntityTypes, ReportType, AggregateInterval
from gcm.inv.reporting.core.Runners.investmentsreporting import InvestmentsReportRunner
from gcm.Scenario.scenario import Scenario


class AggregatePerformanceQualityReport(ReportingRunnerBase):

    def __init__(self, runner, as_of_date, acronyms):
        super().__init__(runner=runner)
        self._as_of_date = as_of_date
        self._as_of_date = as_of_date
        self._portfolio = Portfolio(acronyms=acronyms)
        self._entity_type = 'FUND'
        self._portfolio_acronym = None
        self.__all_holdings = None
        self.__all_pub_port_dimn = None

    @property
    def _all_holdings(self):
        if self.__all_holdings is None:
            holdings = self._portfolio.get_holdings(allocation_date=self._as_of_date)
            self.__all_holdings = holdings[['Acronym', 'InvestmentGroupName', 'PctNav']]
        return self.__all_holdings

    @property
    def _all_acronyms(self):
        return self._all_holdings['Acronym'].unique().tolist()

    @property
    def _holdings(self):
        return self._all_holdings[self._all_holdings['Acronym'] == self._portfolio_acronym]

    @property
    def _all_pub_port_dimn(self):
        if self.__all_pub_port_dimn is None:
            dimn = self._portfolio.get_dimensions()
            self.__all_pub_port_dimn = dimn[['Acronym', 'MasterId']].rename(columns={'MasterId': 'PubPortfolioId'})
        return self.__all_pub_port_dimn

    @property
    def _pub_portfolio_id(self):
        port_dimn = self._all_pub_port_dimn[self.__all_pub_port_dimn['Acronym'] == self._portfolio_acronym]
        return port_dimn['PubPortfolioId'].squeeze()

    @functools.lru_cache(maxsize=None)
    def download_performance_quality_report_inputs(self, fund_name) -> dict:
        location = "lab/rqs/azurefunctiondata"
        read_params = AzureDataLakeDao.create_get_data_params(
            location,
            fund_name.replace('/', '') + "_performance_quality_report_report_analytics.json",
            retry=False,
        )

        try:
            file = self._runner.execute(
                params=read_params,
                source=DaoSource.DataLake,
                operation=lambda dao, params: dao.get_data(read_params)
            )
            return json.loads(file.content)
        except Exception:
            return None

    def _aggregate_portfolio_summary(self, item):
        fund_summaries = pd.DataFrame()
        holdings = self._holdings.copy()

        if holdings['PctNav'].sum().round(2) == 0:
            return pd.DataFrame()

        for fund_name in holdings['InvestmentGroupName']:
            json_inputs = self.download_performance_quality_report_inputs(fund_name=fund_name)
            if json_inputs is not None:
                inputs = pd.read_json(json_inputs[item], orient='index').reset_index()
                inputs.rename(columns={'index': 'Period'}, inplace=True)
                fund_summary = pd.melt(inputs, var_name='Field', value_name='Value', id_vars=['Period'])
                fund_summary['InvestmentGroupName'] = fund_name
                fund_summaries = fund_summaries.append(fund_summary)

        if len(fund_summaries) == 0:
            return pd.DataFrame()

        portfolio_summary = fund_summaries.merge(holdings, on='InvestmentGroupName', how='left')
        portfolio_summary = portfolio_summary[pd.to_numeric(portfolio_summary['Value'], errors='coerce').notnull()]

        if len(portfolio_summary) == 0:
            return pd.DataFrame()

        total_nav_by_group = portfolio_summary.groupby(['Period', 'Field'], as_index=False)['PctNav'].sum()
        total_nav_by_group.rename(columns={'PctNav': 'TotalNav'}, inplace=True)
        portfolio_summary = portfolio_summary.merge(total_nav_by_group, on=['Period', 'Field'], how='left')

        if total_nav_by_group['TotalNav'].sum() == 0:
            return pd.DataFrame()

        portfolio_summary['UnadjContrib'] = portfolio_summary['Value'] * portfolio_summary['PctNav']
        portfolio_summary['Contrib'] = portfolio_summary['UnadjContrib'] / portfolio_summary['TotalNav']

        portfolio_summary.drop(columns={'PctNav'}, inplace=True)
        portfolio_summary = portfolio_summary.groupby(['Field', 'Period'], as_index=False)['Contrib'].sum()
        portfolio_summary = portfolio_summary.pivot_table(index='Period', columns='Field', values='Contrib')
        portfolio_summary = portfolio_summary.round(2)

        original_columns = pd.DataFrame(columns=inputs.columns[1:])
        original_rows = inputs['Period']
        portfolio_summary = pd.concat([original_columns, portfolio_summary])
        index = original_rows.to_frame()
        portfolio_summary = index.merge(portfolio_summary, left_on='Period', right_index=True, how='left')
        portfolio_summary = portfolio_summary.set_index('Period')
        return portfolio_summary

    def generate_performance_quality_report(self):
        header_info = pd.DataFrame({'header_info': [self._portfolio_acronym,
                                                    'ARS FUND - Cap-Weighted Pro Forma (Current Weights)',
                                                    self._as_of_date]})
        benchmark_summary = self._aggregate_portfolio_summary('benchmark_summary')
        absolute_return_benchmark = pd.DataFrame({'absolute_return_benchmark': [' ']})
        peer_group_heading = pd.DataFrame({'peer_group_heading': ['v. GCM Peer']})
        eurekahedge_benchmark_heading = pd.DataFrame({'eurekahedge_benchmark_heading': ['v. EHI Index']})
        peer_ptile_1_heading = pd.DataFrame({'peer_ptile_1_heading': ['Primary Peer']})
        peer_ptile_2_heading = pd.DataFrame({'peer_ptile_2_heading': ['Secondary Peer']})
        performance_stability_fund_summary = self._aggregate_portfolio_summary('performance_stability_fund_summary')
        performance_stability_peer_summary = self._aggregate_portfolio_summary('performance_stability_peer_summary')
        rba_summary = self._aggregate_portfolio_summary('rba_summary')
        pba_mtd = self._aggregate_portfolio_summary('pba_mtd')
        pba_qtd = self._aggregate_portfolio_summary('pba_qtd')
        pba_ytd = self._aggregate_portfolio_summary('pba_ytd')
        shortfall_summary = pd.DataFrame({'Trigger': '', 'Drawdown': '', 'Pass/Fail': ''}, index=['SCL'])
        risk_model_expectations = self._aggregate_portfolio_summary('risk_model_expectations')
        exposure_summary = self._aggregate_portfolio_summary('exposure_summary')
        latest_exposure_heading = pd.DataFrame({'latest_exposure_heading': ['Latest']})

        input_data = {
            "header_info": header_info,
            "benchmark_summary": benchmark_summary,
            "absolute_return_benchmark": absolute_return_benchmark,
            "peer_group_heading": peer_group_heading,
            "eurekahedge_benchmark_heading": eurekahedge_benchmark_heading,
            "peer_ptile_1_heading": peer_ptile_1_heading,
            "peer_ptile_2_heading": peer_ptile_2_heading,
            "performance_stability_fund_summary": performance_stability_fund_summary,
            "performance_stability_peer_summary": performance_stability_peer_summary,
            "rba_summary": rba_summary,
            "pba_mtd": pba_mtd,
            "pba_qtd": pba_qtd,
            "pba_ytd": pba_ytd,
            "shortfall_summary": shortfall_summary,
            "risk_model_expectations": risk_model_expectations,
            "exposure_summary": exposure_summary,
            "latest_exposure_heading": latest_exposure_heading,
        }

        as_of_date = dt.datetime.combine(self._as_of_date, dt.datetime.min.time())
        with Scenario(asofdate=as_of_date).context():
            InvestmentsReportRunner().execute(
                data=input_data,
                template="PFUND_PerformanceQuality_Template.xlsx",
                save=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.portfolio,
                entity_name=self._portfolio_acronym,
                entity_display_name=self._portfolio_acronym.replace('/', ''),
                entity_ids=[self._pub_portfolio_id.item()],
                entity_source=DaoSource.PubDwh,
                report_name='Performance Quality',
                report_type=ReportType.Risk,
                aggregate_intervals=AggregateInterval.MTD,
            )

        return True

    def generate_all_performance_quality_reports(self):
        for acronym in self._all_acronyms:
            self._portfolio_acronym = acronym
            self.generate_performance_quality_report()

    def run(self, **kwargs):
        self.generate_all_performance_quality_reports()
        return True
