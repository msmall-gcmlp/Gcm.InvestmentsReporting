import functools
import json
import pandas as pd
from gcm.Dao.DaoSources import DaoSource
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from .reporting_runner_base import ReportingRunnerBase
from gcm.inv.dataprovider.portfolio_holdings import PortfolioHoldings
from gcm.inv.dataprovider.pub_dwh.pub_portfolio_holdings import PubPortfolioHoldingsQuery
from gcm.inv.dataprovider.entity_master import EntityMaster
from gcm.inv.reporting.core.ReportStructure.report_structure import ReportingEntityTypes
from gcm.inv.reporting.core.Runners.investmentsreporting import InvestmentsReportRunner
from gcm.Scenario.scenario import Scenario


class AggregatePerformanceQualityReport(ReportingRunnerBase):

    def __init__(self, runner, as_of_date, acronym, params):
        super().__init__(runner=runner)
        self._as_of_date = as_of_date
        self._params = params
        pub_portfolio_holdings_query = PubPortfolioHoldingsQuery(runner=runner, as_of_date=as_of_date)
        entity_master = EntityMaster(runner=runner, as_of_date=as_of_date)
        self._as_of_date = as_of_date
        self._portfolio_holdings = PortfolioHoldings(pub_portfolio_holdings_query=pub_portfolio_holdings_query,
                                                     entity_master=entity_master)
        self._portfolio_acronym = acronym
        self._entity_type = 'FUND'

    @property
    def _holdings(self):
        holdings = self._portfolio_holdings.get_portfolio_holdings(allocation_date=self._as_of_date,
                                                                   portfolio_acronyms=self._portfolio_acronym)
        return holdings[['InvestmentGroupName', 'PctNav']]

    @functools.lru_cache(maxsize=None)
    def download_performance_quality_report_inputs(self, fund_name) -> dict:
        location = "lab/rqs/azurefunctiondata"
        read_params = AzureDataLakeDao.create_get_data_params(
            location,
            fund_name + "_performance_quality_report_report_analytics.json",
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
        for fund_name in holdings['InvestmentGroupName']:
            json_inputs = self.download_performance_quality_report_inputs(fund_name=fund_name)
            if json_inputs is not None:
                inputs = pd.read_json(json_inputs[item], orient='index').reset_index()
                inputs.rename(columns={'index': 'Period'}, inplace=True)
                fund_summary = pd.melt(inputs, var_name='Field', value_name='Value', id_vars=['Period'])
                fund_summary['InvestmentGroupName'] = fund_name
                fund_summaries = fund_summaries.append(fund_summary)

        portfolio_summary = fund_summaries.merge(holdings, on='InvestmentGroupName', how='left')
        portfolio_summary = portfolio_summary[pd.to_numeric(portfolio_summary['Value'], errors='coerce').notnull()]

        total_nav_by_group = portfolio_summary.groupby(['Period', 'Field'], as_index=False)['PctNav'].sum()
        total_nav_by_group.rename(columns={'PctNav': 'TotalNav'}, inplace=True)
        portfolio_summary = portfolio_summary.merge(total_nav_by_group, on=['Period', 'Field'], how='left')

        portfolio_summary['UnadjContrib'] = portfolio_summary['Value'] * portfolio_summary['PctNav']
        portfolio_summary['Contrib'] = portfolio_summary['UnadjContrib'] / portfolio_summary['TotalNav']

        portfolio_summary.drop(columns={'PctNav'}, inplace=True)
        portfolio_summary = portfolio_summary.groupby(['Field', 'Period'], as_index=False)['Contrib'].sum()
        portfolio_summary = portfolio_summary.pivot_table(index='Period', columns='Field', values='Contrib')
        portfolio_summary = portfolio_summary.round(2)

        original_columns = pd.DataFrame(columns=inputs.columns[1:])
        original_rows = inputs['Period']
        portfolio_summary = pd.concat([original_columns, portfolio_summary])
        portfolio_summary = portfolio_summary.loc[original_rows]
        return portfolio_summary

    def generate_performance_quality_report(self):
        header_info = pd.DataFrame({'header_info': [self._portfolio_acronym,
                                                    'FUND',
                                                    self._as_of_date]})
        benchmark_summary = self._aggregate_portfolio_summary('benchmark_summary')
        absolute_return_benchmark = pd.DataFrame({'absolute_return_benchmark': [' ']})
        peer_group_heading = pd.DataFrame({'peer_group_heading': ['v. GCM Peer']})
        eurekahedge_benchmark_heading = pd.DataFrame({'eurekahedge_benchmark_heading': ['v. EHI Index']})
        peer_ptile_1_heading = pd.DataFrame({'peer_ptile_1_heading': ['Primary Peer']})
        peer_ptile_2_heading = pd.DataFrame({'peer_ptile_2_heading': ['']})
        performance_stability_fund_summary = self._aggregate_portfolio_summary('performance_stability_fund_summary')
        performance_stability_peer_summary = self._aggregate_portfolio_summary('performance_stability_peer_summary')
        rba_summary = self._aggregate_portfolio_summary('rba_summary')
        pba_mtd = self._aggregate_portfolio_summary('pba_mtd')
        pba_qtd = self._aggregate_portfolio_summary('pba_qtd')
        pba_ytd = self._aggregate_portfolio_summary('pba_ytd')
        shortfall_summary = pd.DataFrame({'shortfall_summary': []})
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

        with Scenario(asofdate=self._as_of_date).context():
            report_name = "FUND_PerformanceQuality_" + self._portfolio_acronym.replace('/', '')

            #TODO GIP is placeholder
            InvestmentsReportRunner().execute(
                data=input_data,
                template="PFUND_PerformanceQuality_Template.xlsx",
                save=True,
                report_name=report_name,
                runner=self._runner,
                entity_name='GIP',
                entity_display_name=self._entity_type,
                entity_type=ReportingEntityTypes.portfolio,
                entity_source=DaoSource.PubDwh,
            )

        return True

    def run(self, **kwargs):
        self.generate_performance_quality_report()
        return True
