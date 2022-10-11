import json
import logging
import datetime as dt
import numpy as np
from functools import cached_property
from gcm.Dao.DaoRunner import DaoRunner
import pandas as pd
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.dataprovider.strategy_benchmark import StrategyBenchmark
from gcm.inv.dataprovider.investment_group import InvestmentGroup
from gcm.inv.dataprovider.entity_master import EntityMaster
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from pandas._libs.tslibs.offsets import relativedelta
from gcm.inv.quantlib.enum_source import Periodicity, PeriodicROR
from gcm.inv.reporting.core.ReportStructure.report_structure import (
    ReportingEntityTypes,
    ReportType,
    AggregateInterval, ReportVertical,
)
from gcm.inv.reporting.core.Runners.investmentsreporting import (
    InvestmentsReportRunner,
)
from gcm.Scenario.scenario import Scenario
from gcm.inv.quantlib.timeseries.analytics import Analytics
from gcm.inv.reporting.core.reporting_runner_base import (
    ReportingRunnerBase,
)
from gcm.inv.dataprovider.factor import Factor
from gcm.inv.quantlib.timeseries.transformer.aggregate_from_daily import (
    AggregateFromDaily,
)


class PerformanceScreenerReport(ReportingRunnerBase):
    def __init__(self, peer_group, trailing_months=36):
        super().__init__(runner=Scenario.get_attribute("runner"))
        self._as_of_date = Scenario.get_attribute("as_of_date")
        self._end_date = self._as_of_date
        self._trailing_months = trailing_months
        self._start_date = self._as_of_date - relativedelta(years=self._trailing_months / 12)
        self._analytics = Analytics()
        self._strategy_benchmark = StrategyBenchmark()
        self._peer_group = peer_group
        self._summary_data_location = "raw/investmentsreporting/summarydata/xpfund_performance_screener"

    @cached_property
    def _constituents(self):
        return self._get_peer_constituents()

    @cached_property
    def _all_constituent_returns(self):
        inv_group_ids = self._constituents['InvestmentGroupId'].unique().tolist()
        returns = self._get_constituent_returns(investment_group_ids=inv_group_ids)
        return returns

    @cached_property
    def _updated_constituent_returns(self, min_required_returns=18):
        as_of_month = dt.date(year=self._as_of_date.year, month=self._as_of_date.month, day=1)
        as_of_month = pd.to_datetime(as_of_month)

        if as_of_month not in self._all_constituent_returns.index:
            return None

        updated_funds = self._all_constituent_returns.loc[as_of_month].dropna().index
        returns = self._all_constituent_returns.loc[:, updated_funds]
        sufficient_track = ~returns[-min_required_returns:].isna().any()
        sufficient_track_funds = sufficient_track[sufficient_track].index.tolist()
        returns = returns.loc[:, sufficient_track_funds]
        return returns

    @cached_property
    def _absolute_benchmark_returns(self):
        ids = self._constituents['InvestmentGroupId'].unique().tolist()
        inv_group = InvestmentGroup(investment_group_ids=ids)
        abs_bmrk_returns = inv_group.get_absolute_benchmark_returns(start_date=self._start_date,
                                                                    end_date=self._end_date)
        abs_bmrk_returns = AggregateFromDaily().transform(
            data=abs_bmrk_returns,
            method="geometric",
            period=Periodicity.Monthly,
            first_of_day=True
        )
        group_dimensions = inv_group.get_dimensions()
        id_name_map = dict(zip(group_dimensions['InvestmentGroupId'], group_dimensions['InvestmentGroupName']))
        abs_bmrk_returns.columns = [id_name_map.get(item, item) for item in abs_bmrk_returns.columns]
        return abs_bmrk_returns

    @cached_property
    def _rf_returns(self):
        returns = self._get_monthly_factor_returns(ticker="SBMMTB1 Index")
        return returns

    @cached_property
    def _peer_benchmark_return(self):
        returns = self._get_monthly_factor_returns(ticker=self._peer_benchmark_ticker)
        return returns

    def _validate_inputs(self):
        pass

    def _get_altsoft_investment_names(self, entities):
        alt_soft_entities = entities[entities['SourceName'].str.contains('AltSoft.')]
        alt_soft_entities = alt_soft_entities[alt_soft_entities['IsReportingInvestment']]
        alt_soft_entities = alt_soft_entities[['InvestmentName', 'InvestmentGroupName']]
        return alt_soft_entities

    def _get_peer_constituents(self):
        constituents = self._strategy_benchmark.get_altsoft_peer_constituents(peer_names=self._peer_group)
        ids = constituents['InvestmentGroupId']
        investment_group = InvestmentGroup(investment_group_ids=ids)
        dimensions = investment_group.get_dimensions()
        dimensions = dimensions[['InvestmentGroupId', 'InvestmentGroupName', 'InvestmentStatus']]

        constituents = constituents.merge(dimensions, on=['InvestmentGroupId'], how='inner')

        entities = EntityMaster().get_investment_entities()
        reporting_inv_names = self._get_altsoft_investment_names(entities)
        constituents = constituents.merge(reporting_inv_names, on=['InvestmentGroupName'], how='left')

        constituents = constituents[['InvestmentGroupId', 'InvestmentGroupName', 'InvestmentName', 'InvestmentStatus']]
        constituents = constituents.drop_duplicates()

        return constituents

    def _get_constituent_returns(self, investment_group_ids):
        investment_group = InvestmentGroup(investment_group_ids=investment_group_ids)
        returns = investment_group.get_monthly_returns(start_date=self._start_date,
                                                       end_date=self._as_of_date,
                                                       wide=True)
        return returns

    def _get_monthly_factor_returns(self, ticker):
        factors = Factor(tickers=[ticker])
        returns = factors.get_returns(
            start_date=self._start_date,
            end_date=self._end_date,
            fill_na=True,
        )
        if len(returns) > 0:
            returns = AggregateFromDaily().transform(
                data=returns,
                method="geometric",
                period=Periodicity.Monthly,
            )
            returns.index = [dt.datetime(x.year, x.month, 1) for x in returns.index.tolist()]
            return returns
        else:
            return pd.DataFrame()

    def _calculate_annualized_return(self, returns):
        ann_factor = np.minimum(1, 12 / returns.notna().sum())
        cumulative_returns = self._analytics.compute_periodic_return(
            ror=returns,
            period=PeriodicROR.ITD,
            as_of_date=self._as_of_date,
            method="geometric",
        )

        annualized_returns = (1 + cumulative_returns) ** (ann_factor) - 1

        ror = annualized_returns.to_frame('Return')
        return ror

    def _get_annualized_return(self):
        returns = self._calculate_annualized_return(returns=self._updated_constituent_returns)
        return returns

    def _get_annualized_vol(self):
        # vol = self._analytics.compute_trailing_vol(
        #     ror=self._constituent_returns,
        #     window=self._trailing_months,
        #     as_of_date=self._as_of_date,
        #     periodicity=Periodicity.Monthly,
        #     annualize=True,
        # )
        vol = self._updated_constituent_returns.std() * np.sqrt(12)
        vol = vol.to_frame('Vol')
        return vol

    def _get_sharpe_ratio(self):
        # sharpe = self._analytics.compute_trailing_sharpe_ratio(
        #     ror=self._updated_constituent_returns,
        #     rf_ror=self._rf_returns,
        #     window=self._trailing_months,
        #     as_of_date=self._as_of_date,
        #     periodicity=Periodicity.Monthly,
        # )
        #
        rf = (self._rf_returns.mean() * 12).squeeze()
        excess_return = (self._get_annualized_return() - rf)
        vol = self._get_annualized_vol()
        sharpe = excess_return['Return'] / vol['Vol']
        sharpe = sharpe.to_frame('Sharpe')
        return sharpe

    def _get_return_in_peer_stress_months(self):
        avg_peer_return = self._updated_constituent_returns.median(axis=1)
        number_months = round(self._trailing_months * 0.10)
        worst_peer_months = avg_peer_return.sort_values()[0:number_months]
        worst_peer_dates = worst_peer_months.index.tolist()
        peer_stress_ror = self._updated_constituent_returns.loc[worst_peer_dates].mean(axis=0)
        peer_stress_ror = peer_stress_ror.to_frame('PeerStressRor')
        return peer_stress_ror

    def _get_max_1mo_return(self):
        max_ror = self._updated_constituent_returns.max(axis=0)
        max_ror = max_ror.to_frame('MaxRor')
        return max_ror

    @staticmethod
    def _calculate_peer_rankings(standalone_metrics, relative_metrics):
        #TODO replace with Daivik ranking
        rankings = standalone_metrics.merge(relative_metrics, left_index=True, right_index=True)
        rankings = rankings.reindex(['Sharpe', 'PeerStressRor', 'MaxRor', 'Excess'], axis=1)
        # rankings = rankings.dropna()
        rankings = rankings.rank(axis=0, ascending=True)
        rankings = rankings.mean(axis=1).sort_values(ascending=True)

        # need to ensure no points are identical. randomly break ties
        rankings = rankings + np.random.random(rankings.shape[0]) / 1e3
        rankings = rankings.reset_index().rename(columns={'index': 'InvestmentGroupName', 0: 'Points'})
        rankings['Quartile'] = pd.qcut(x=rankings['Points'], q=[0, 0.25, 0.50, 0.75, 1], labels=[4, 3, 2, 1])
        rankings['Rank'] = rankings['Points'].rank(pct=False, ascending=False)
        rankings = rankings.sort_values(['Rank', 'InvestmentGroupName'], ascending=[True, True])
        rankings = rankings[['Rank', 'InvestmentGroupName', 'Quartile']]
        return rankings

    def get_header_info(self):
        header = pd.DataFrame(
            {
                "header_info": [
                    self._peer_group,
                    'ARS',
                    self._as_of_date,
                ]
            }
        )
        return header

    def build_constituent_count_summary(self):
        counts = [self._updated_constituent_returns.shape[1], self._constituents.shape[0]]

        counts = pd.DataFrame({'counts': counts})
        return counts

    def build_standalone_metrics_summary(self):
        ror = self._get_annualized_return()
        vol = self._get_annualized_vol()
        sharpe = self._get_sharpe_ratio()
        peer_stress_ror = self._get_return_in_peer_stress_months()
        max_ror = self._get_max_1mo_return()

        summary = ror.merge(vol, how='left', left_index=True, right_index=True)
        summary = summary.merge(sharpe, how='left', left_index=True, right_index=True)
        summary = summary.merge(peer_stress_ror, how='left', left_index=True, right_index=True)
        summary = summary.merge(max_ror, how='left', left_index=True, right_index=True)
        summary = summary.astype(float).round(2)

        return summary

    def _get_arb_excess_return(self):
        fund_return = self._get_annualized_return()
        fund_return.rename(columns={"Return": "Fund"}, inplace=True)

        funds = self._updated_constituent_returns.columns
        benchmark_returns = self._absolute_benchmark_returns.reindex(funds, axis=1)

        benchmark_returns[self._updated_constituent_returns.isna()] = None

        bmrk_return = self._calculate_annualized_return(returns=benchmark_returns)
        bmrk_return.rename(columns={'Return': 'Bmrk'}, inplace=True)
        # bmrk_return = self._analytics.compute_trailing_return(
        #     ror=self._absolute_benchmark_returns,
        #     window=self._trailing_months,
        #     as_of_date=self._as_of_date,
        #     method="geometric",
        #     periodicity=Periodicity.Monthly,
        #     annualize=True,
        # )
        # bmrk_return = bmrk_return.to_frame('Bmrk')

        fund_bmrk_return = fund_return.merge(bmrk_return, left_index=True, right_index=True)
        fund_bmrk_return['Excess'] = fund_bmrk_return['Fund'] - fund_bmrk_return['Bmrk']
        excess_return = fund_bmrk_return['Excess'].to_frame('Excess')
        excess_return['Excess'] = excess_return['Excess'].astype(float).round(2)
        return excess_return

    def _get_arb_r_squared(self):
        funds = self._updated_constituent_returns.columns.tolist()
        r2_summary = pd.DataFrame(index=funds)
        for fund in funds:
            fund_return = self._updated_constituent_returns[fund].to_frame('Fund')
            fund_return = fund_return.astype(float)
            if fund in self._absolute_benchmark_returns.columns:
                bmrk_return = self._absolute_benchmark_returns[fund].to_frame('Bmrk')
                data = fund_return.merge(bmrk_return, left_index=True, right_index=True)
                correlation = data.corr(min_periods=18)
                r2 = correlation.loc['Fund', 'Bmrk'] ** 2
                r2 = r2.round(2)
                r2_summary.loc[fund, 'R2'] = r2
        return r2_summary

    def build_absolute_return_benchmark_summary(self):
        excess_return = self._get_arb_excess_return()
        r_squared = self._get_arb_r_squared()
        summary = excess_return.merge(r_squared, how='left', left_index=True, right_index=True)

        if summary.shape[1] == 1:
            return pd.DataFrame(columns=['Excess', 'ExcessQtile', 'R2'], index=summary.index)

        summary['ExcessQtile'] = pd.qcut(x=summary['Excess'], q=[0, 0.25, 0.50, 0.75, 1], labels=[4, 3, 2, 1])
        summary['ExcessQtile'] = summary['ExcessQtile'].astype(float)
        summary = summary[['Excess', 'ExcessQtile', 'R2']]
        return summary

    def build_rba_excess_return_summary(self):
        inv_group_ids = self._constituents['InvestmentGroupId'].unique().tolist()
        inv_group = InvestmentGroup(investment_group_ids=inv_group_ids)
        rba = inv_group.get_rba_return_decomposition_by_date(
            start_date=self._start_date, end_date=self._end_date, frequency='M', window=36,
            factor_filter=['NON_FACTOR'])
        excess = self._calculate_annualized_return(returns=rba)
        excess = excess.reset_index()
        excess = excess[excess['AggLevel'] == 'NON_FACTOR']
        excess.columns = excess.columns[:-1].tolist() + ['Excess']
        excess.drop(columns={'InvestmentGroupId', 'FactorGroup1'}, inplace=True)
        excess = excess[excess['AggLevel'] == 'NON_FACTOR'].drop(columns='AggLevel')
        excess = excess.set_index('InvestmentGroupName')
        excess.rename(columns={'Excess': 'RbaAlpha'}, inplace=True)

        # need to ensure no points are identical. randomly break ties
        excess['RbaAlpha'] = excess['RbaAlpha'] + np.random.random(excess.shape[0]) / 1e3

        quartiles = pd.qcut(x=excess['RbaAlpha'], q=[0, 0.25, 0.50, 0.75, 1], labels=[4, 3, 2, 1])
        quartiles.name = 'RbaAlphaQtile'

        summary = excess.merge(quartiles, left_index=True, right_index=True)
        summary['RbaAlphaQtile'] = summary['RbaAlphaQtile'].astype(float)

        return summary

    def build_rba_risk_decomposition_summary(self):
        inv_group_ids = self._constituents['InvestmentGroupId'].unique().tolist()
        inv_group = InvestmentGroup(investment_group_ids=inv_group_ids)
        rba_risk_decomp = inv_group.get_average_risk_decomp_by_group(
            start_date=self._start_date, end_date=self._end_date, frequency='M', window=36,
            group_type='FactorGroup1', wide=False)
        rba_risk_decomp.drop(columns='InvestmentGroupId', inplace=True)
        rba_risk_decomp = rba_risk_decomp.pivot(index='InvestmentGroupName', columns='FactorGroup1')
        rba_risk_decomp.columns = rba_risk_decomp.columns.droplevel(0)
        rba_risk_decomp = rba_risk_decomp.reindex(['SYSTEMATIC', 'X_ASSET_CLASS', 'PUBLIC_LS', 'NON_FACTOR'], axis=1)
        rba_risk_decomp = rba_risk_decomp.fillna(0)
        return rba_risk_decomp

    def build_summary_table(self, rankings, stats):
        qtile_headings = pd.DataFrame({'Rank': [-100] * 4,
                                       'InvestmentGroupName': ['Quartile ' + str(x) for x in [1, 2, 3, 4]],
                                       'Quartile': [1, 2, 3, 4]})
        rankings = pd.concat([rankings, qtile_headings])

        summary = rankings.merge(stats, left_on=['InvestmentGroupName'], right_index=True, how='left')
        statuses = self._constituents[['InvestmentGroupName', 'InvestmentStatus']]
        summary = summary.merge(statuses, on='InvestmentGroupName', how='left')

        column_order = ['Rank', 'InvestmentGroupName'] + summary.columns[3:].tolist()

        summary = summary.sort_values(['Quartile', 'Rank'])
        summary = summary[column_order]
        summary['Rank'] = [round(x, 0) if x != -100 else np.NAN for x in summary['Rank']]

        name_overrides = dict(zip(self._constituents['InvestmentGroupName'], self._constituents['InvestmentName']))
        summary["InvestmentGroupName"] = summary["InvestmentGroupName"].replace(name_overrides)
        summary["InvestmentGroupName"] = summary["InvestmentGroupName"].str.slice(0, 27)

        return summary

    def build_omitted_fund_summary(self):
        constituents = self._constituents['InvestmentGroupName']
        modelled_constituents = self._updated_constituent_returns.columns.tolist()
        omitted_constituents = constituents[~constituents.isin(modelled_constituents)].dropna().tolist()
        omitted_constituents = sorted(omitted_constituents)

        returns = self._all_constituent_returns
        omitted_fund_returns = returns.loc[:, returns.columns.isin(omitted_constituents)]

        last_return_date = omitted_fund_returns.apply(lambda x: x.last_valid_index())
        last_return_date = last_return_date.to_frame('LastReturnDate')

        return_count = omitted_fund_returns.count(axis=0)
        return_count = return_count.to_frame('NoReturns')

        #TODO adjust
        partial_return = omitted_fund_returns.mean() * 12
        partial_return = partial_return.to_frame('PartialReturn')

        summary = pd.DataFrame(index=omitted_constituents)
        summary = summary.merge(last_return_date, left_index=True, right_index=True, how='left')
        summary = summary.merge(return_count, left_index=True, right_index=True, how='left')
        summary = summary.merge(partial_return, left_index=True, right_index=True, how='left')

        summary = summary.reset_index()

        return summary

    def generate_performance_screener_report(self):
        # if not self._validate_inputs():
        #     return 'Invalid inputs'

        logging.info("Generating report for: " + self._peer_group)

        header_info = self.get_header_info()

        if self._updated_constituent_returns is None:
            return "No peers have returns through as of date"

        constituent_counts = self.build_constituent_count_summary()
        standalone_metrics = self.build_standalone_metrics_summary()
        relative_metrics = self.build_absolute_return_benchmark_summary()
        rankings = self._calculate_peer_rankings(standalone_metrics=standalone_metrics,
                                                 relative_metrics=relative_metrics)

        rba_excess_return_summary = self.build_rba_excess_return_summary()
        rba_risk_decomposition_summary = self.build_rba_risk_decomposition_summary()

        summary = standalone_metrics.merge(relative_metrics, how='left', left_index=True, right_index=True)
        summary = summary.merge(rba_excess_return_summary, how='left', left_index=True, right_index=True)
        summary = summary.merge(rba_risk_decomposition_summary, how='left', left_index=True, right_index=True)

        summary_table = self.build_summary_table(rankings=rankings, stats=summary)
        omitted_funds = self.build_omitted_fund_summary()

        rankings_max_row = 11 + summary_table.shape[0]
        omitted_funds_max_row = 11 + omitted_funds.shape[0]
        print_areas = {'Rankings': 'B1:Q' + str(rankings_max_row),
                       'Omitted Funds': 'B1:E' + str(omitted_funds_max_row)}

        logging.info('Report summary data generated for: ' + self._peer_group)

        input_data = {
            "header_info1": header_info,
            "header_info2": header_info,
            "summary_table": summary_table,
            "omitted_funds": omitted_funds,
            "constituents1": constituent_counts,
            "constituents2": constituent_counts,
        }

        input_data_json = {
            "header_info1": header_info.to_json(orient='index'),
            "header_info2": header_info.to_json(orient='index'),
            "summary_table": summary_table.to_json(orient='index'),
            "omitted_funds": omitted_funds.to_json(orient='index'),
            "constituents1": constituent_counts.to_json(orient='index'),
        }

        data_to_write = json.dumps(input_data_json)
        as_of_date = self._as_of_date.strftime("%Y-%m-%d")
        write_params = AzureDataLakeDao.create_get_data_params(
            self._summary_data_location,
            self._peer_group.replace("/", "") + as_of_date + ".json",
            retry=False,
        )
        self._runner.execute(
            params=write_params,
            source=DaoSource.DataLake,
            operation=lambda dao, params: dao.post_data(params, data_to_write),
        )

        logging.info("JSON stored to DataLake for: " + self._peer_group)

        as_of_date = dt.datetime.combine(self._as_of_date, dt.datetime.min.time())
        entity_name = self._peer_group.replace("/", "").replace("GCM ", "") + ' Peer'

        with Scenario(as_of_date=as_of_date).context():
            InvestmentsReportRunner().execute(
                data=input_data,
                template="XPFUND_Performance_Screener_Template.xlsx",
                save=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.cross_entity,
                entity_name=entity_name,
                entity_display_name=entity_name,
                report_name="XPFUND Performance Screener",
                report_type=ReportType.Performance,
                report_vertical=ReportVertical.ARS,
                report_frequency="Monthly",
                aggregate_intervals=AggregateInterval.MTD,
                print_areas=print_areas,
                # output_dir="cleansed/investmentsreporting/printedexcels/",
                # report_output_source=DaoSource.DataLake,
            )

        logging.info("Excel stored to DataLake for: " + self._peer_group)

    def run(self, **kwargs):
        self.generate_performance_screener_report()
        return self._peer_group + " Complete"


if __name__ == "__main__":
    peer_groups = ["GCM Multi-PM"]
    with Scenario(runner=DaoRunner(), as_of_date=dt.date(2022, 6, 30)).context():
        for peer_group in peer_groups:
            PerformanceScreenerReport(peer_group=peer_group).execute()
