import json
import logging

import pandas as pd
import datetime as dt
from gcm.inv.dataprovider.investment_group import InvestmentGroup
from gcm.Dao.DaoSources import DaoSource
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from gcm.inv.reporting.core.ReportStructure.report_structure import ReportingEntityTypes, ReportType, AggregateInterval
from gcm.inv.reporting.core.Runners.investmentsreporting import InvestmentsReportRunner
from gcm.Scenario.scenario import Scenario
from gcm.inv.quantlib.enum_source import PeriodicROR, Periodicity
from gcm.inv.quantlib.timeseries.analytics import Analytics
from .reporting_runner_base import ReportingRunnerBase



class EofReturnBasedAttributionReport(ReportingRunnerBase):

    def __init__(self):
        super().__init__(runner=Scenario.get_attribute('runner'))
        self._as_of_date = Scenario.get_attribute('as_of_date')
        self._periodicity = Scenario.get_attribute('periodicity')
        self._analytics = Analytics()
        self._underlying_data_location = "raw/investmentsreporting/underlyingdata/eof_rba"
        self._summary_data_location = "raw/investmentsreporting/summarydata/eof_rba"

    @property
    def _start_date(self):
        if self._periodicity == PeriodicROR.ITD:
            start_date = dt.date(2020, 10, 1)
        elif self._periodicity == PeriodicROR.YTD:
            start_date = dt.date(self._as_of_date.year, 1, 1)
        return start_date

    @property
    def _end_date(self):
        return self._as_of_date

    def _get_rba_summary(self):
        subtotal_factors = ['SYSTEMATIC', 'X_ASSET_CLASS', 'PUBLIC_LS', 'NON_FACTOR']
        non_subtotal_factors = ['INDUSTRY', 'REGION',
                                'LS_EQUITY_VALUE_GROUP',
                                'LS_EQUITY_GROWTH_GROUP',
                                'LS_EQUITY_MOMENTUM_GROUP',
                                'LS_EQUITY_QUALITY_GROUP',
                                'LS_EQUITY_SIZE_GROUP',
                                'LS_EQUITY_RESIDUAL_VOL_GROUP',
                                'LS_EQUITY_OTHER',
                                'NON_FACTOR_SECURITY_SELECTION_PUBLICS',
                                'NON_FACTOR_OUTLIER_EFFECTS'
                                ]
        rba_subtotals = self._inv_group.get_rba_return_decomposition_by_date(start_date=self._start_date,
                                                                             end_date=self._end_date,
                                                                             factor_filter=subtotal_factors,
                                                                             frequency="M")

        rba_non_subtotals = self._inv_group.get_rba_return_decomposition_by_date(start_date=self._start_date,
                                                                                 end_date=self._end_date,
                                                                                 factor_filter=non_subtotal_factors,
                                                                                 frequency="M")

        rba_summary = pd.DataFrame(index=['SYSTEMATIC',
                                          'X_ASSET_CLASS',
                                          'INDUSTRY',
                                          'REGION',
                                          'PUBLIC_LS',
                                          'LS_EQUITY_VALUE_GROUP',
                                          'LS_EQUITY_GROWTH_GROUP',
                                          'LS_EQUITY_MOMENTUM_GROUP',
                                          'LS_EQUITY_QUALITY_GROUP',
                                          'LS_EQUITY_SIZE_GROUP',
                                          'LS_EQUITY_RESIDUAL_VOL_GROUP',
                                          'LS_EQUITY_OTHER',
                                          'NON_FACTOR',
                                          'NON_FACTOR_SECURITY_SELECTION_PUBLICS',
                                          'NON_FACTOR_OUTLIER_EFFECTS'
                                          ])

        fund_names = rba_subtotals.columns.get_level_values(0).unique().tolist()

        for fund in fund_names:
            subtotals = rba_subtotals[fund]
            subtotals.columns = subtotals.columns.droplevel(0).droplevel(0)

            non_subtotals = rba_non_subtotals[fund]
            non_subtotals.columns = non_subtotals.columns.droplevel(0).droplevel(0)

            subtotal_decomp = self._analytics.compute_return_attributions(attribution_ts=subtotals,
                                                                          periodicity=Periodicity.Monthly,
                                                                          as_of_date=self._end_date,
                                                                          window=subtotals.shape[0],
                                                                          annualize=False)

            non_subtotal_decomp = self._analytics.compute_return_attributions(attribution_ts=non_subtotals,
                                                                              periodicity=Periodicity.Monthly,
                                                                              as_of_date=self._end_date,
                                                                              window=non_subtotals.shape[0],
                                                                              annualize=False)

            fund_rba = pd.concat([subtotal_decomp, non_subtotal_decomp], axis=0)

            fund_rba.rename(columns={'CTR': fund}, inplace=True)
            rba_summary = rba_summary.merge(fund_rba, left_index=True, right_index=True, how='left')

        rba_summary = rba_summary.fillna(0)
        rba_summary.index = rba_summary.index + '_RETURN_ATTRIB'
        return rba_summary

    @staticmethod
    def _get_top_line_summary(rba_summary):
        subtotal_factors = ['SYSTEMATIC', 'X_ASSET_CLASS', 'PUBLIC_LS', 'NON_FACTOR']
        subtotal_factors = [x + '_RETURN_ATTRIB' for x in subtotal_factors]
        rba_by_subtotal = rba_summary.loc[subtotal_factors]
        ann_returns = rba_by_subtotal.sum(axis=0).to_frame()
        idios = rba_summary.loc['NON_FACTOR_SECURITY_SELECTION_PUBLICS_RETURN_ATTRIB']
        top_line_summary = pd.concat([ann_returns, idios], axis=1)
        top_line_summary.columns = ['Return', 'Idio Only']
        top_line_summary = top_line_summary.T
        top_line_summary.index = top_line_summary.index + '_TOP_LINE'
        return top_line_summary

    def _get_risk_decomp(self):
        factors = pd.DataFrame(index=['SYSTEMATIC',
                                      'X_ASSET_CLASS',
                                      'INDUSTRY',
                                      'REGION',
                                      'PUBLIC_LS',
                                      'NON_FACTOR'])

        decomp_fg1 = self._inv_group.get_average_risk_decomp_by_group(start_date=self._start_date,
                                                                      end_date=self._end_date,
                                                                      group_type='FactorGroup1',
                                                                      frequency='M',
                                                                      wide=False)

        decomp_fg2 = self._inv_group.get_average_risk_decomp_by_group(start_date=self._start_date,
                                                                      end_date=self._end_date,
                                                                      group_type='FactorGroup2',
                                                                      frequency='M',
                                                                      wide=False)

        decomp_fg1 = decomp_fg1.pivot(index='FactorGroup1', columns='InvestmentGroupName', values='AvgRiskContrib')
        decomp_fg2 = decomp_fg2.pivot(index='FactorGroup2', columns='InvestmentGroupName', values='AvgRiskContrib')
        decomp = pd.concat([decomp_fg1, decomp_fg2], axis=0)
        decomp = factors.merge(decomp, left_index=True, right_index=True, how='left')
        decomp = decomp.fillna(0)
        decomp.index = decomp.index + '_RISK_DECOMP'
        return decomp

    def _get_average_adj_r2(self):
        r2s = self._inv_group.get_average_adj_r2(start_date=self._start_date,
                                                 end_date=self._end_date,
                                                 frequency='M')
        r2s = r2s[['InvestmentGroupName', 'AvgAdjR2']].T
        r2s.columns = r2s.loc['InvestmentGroupName']
        r2s = r2s.drop('InvestmentGroupName')

        return r2s

    def _get_attribution_table_rba(self):
        investment_group_ids = [19224, 23319, 74984]
        self._inv_group = InvestmentGroup(investment_group_ids=investment_group_ids)
        # TODO get returns by summing attributions

        rba_summary = self._get_rba_summary()
        top_line_summary = self._get_top_line_summary(rba_summary=rba_summary)
        risk_decomp = self._get_risk_decomp()
        r2 = self._get_average_adj_r2()
        attribution_table = pd.concat([top_line_summary, rba_summary, risk_decomp, r2])
        attribution_table = pd.concat([attribution_table.columns.to_frame().T, attribution_table])
        return attribution_table

    def _get_factor_performance_tables(self):
        factor_summary = self._factor_returns.summarise_ptd_factor_returns(start_date=self._start_date,
                                                                           end_date=self._end_date,
                                                                           universe=self._universe,
                                                                           period_to_date=self._periodicity)

        market_factors = factor_summary[factor_summary['FactorGroup1'] == 'Market Beta'].drop(columns='FactorGroup1')
        market_factors['FactorGroup2'] = [x.replace("Long ", "") for x in market_factors['FactorGroup2']]
        market_factors = market_factors.rename(columns={"FactorGroup2": "Factor Group"}).drop(columns='Ticker')

        style_factors = factor_summary[factor_summary['FactorGroup1'] != 'Market Beta'].drop(columns='FactorGroup1')
        style_factors = style_factors.rename(columns={"FactorGroup2": "Factor Group"})
        style_factors['Ticker'] = [x.replace(" Index", "") for x in style_factors['Ticker']]
        style_factors = style_factors.drop(columns={'Factor', 'Factor Group', 'Ticker'})
        return market_factors, style_factors

    def _write_report_to_data_lake(self, input_data, input_data_json):
        data_to_write = json.dumps(input_data_json)
        asofdate = self._as_of_date.strftime('%Y-%m-%d')
        write_params = AzureDataLakeDao.create_get_data_params(
            self._summary_data_location,
            "EOF_" + self._periodicity.value + "_RBA_Report_" + asofdate + ".json",
            retry=False,
        )
        self._runner.execute(
            params=write_params,
            source=DaoSource.DataLake,
            operation=lambda dao, params: dao.post_data(params, data_to_write)
        )

        logging.info('JSON stored to DataLake for: ' + "EOF_" + self._periodicity.value)

        as_of_date = dt.datetime.combine(self._as_of_date, dt.datetime.min.time())
        with Scenario(asofdate=as_of_date).context():
            InvestmentsReportRunner().execute(
                data=input_data,
                template="EOF RBA Template.xlsx",
                save=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.manager_fund_group,
                entity_name='Equity Opps Fund Ltd',
                entity_display_name='EOF',
                entity_ids=[19163],
                entity_source=DaoSource.PubDwh,
                report_name='EOF RBA',
                report_type=ReportType.Risk,
                aggregate_intervals=AggregateInterval.MTD,
                output_dir="cleansed/investmentsreporting/printedexcels/",
                report_output_source=DaoSource.DataLake
            )

        logging.info('Excel stored to DataLake for: ' + "EOF_" + self._periodicity.value)

    def generate_rba_report(self):
        attribution_table = self._get_attribution_table_rba()
        #market_factor_summary, style_factor_summary = self._get_factor_performance_tables()

        input_data = {
            "attribution_table": attribution_table,
            #"market_factor_summary": market_factor_summary,
            #"style_factor_summary": style_factor_summary
        }

        input_data_json = {
            "attribution_table": attribution_table.to_json(orient='index'),
            #"market_factor_summary": market_factor_summary.to_json(orient='index'),
            #"style_factor_summary": style_factor_summary.to_json(orient='index')
        }
        self._write_report_to_data_lake(input_data=input_data, input_data_json=input_data_json)

    def run(self, **kwargs):
        self.generate_rba_report()
        return True
