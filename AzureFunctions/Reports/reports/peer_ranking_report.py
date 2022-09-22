import json
import logging
import datetime as dt
import numpy as np
from functools import cached_property

import pandas as pd
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.dataprovider.strategy_benchmark import StrategyBenchmark
from gcm.inv.dataprovider.investment_group import InvestmentGroup
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from pandas._libs.tslibs.offsets import relativedelta
from gcm.inv.quantlib.enum_source import Periodicity
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


class PeerRankingReport(ReportingRunnerBase):
    def __init__(self, peer_group, trailing_months=36):
        super().__init__(runner=Scenario.get_attribute("runner"))
        self._as_of_date = Scenario.get_attribute("as_of_date")
        self._end_date = self._as_of_date
        self._trailing_months = trailing_months
        self._start_date = self._as_of_date - relativedelta(years=self._trailing_months / 12)
        self._analytics = Analytics()
        self._strategy_benchmark = StrategyBenchmark()
        self._peer_group = peer_group
        self._underlying_data_location = "raw/investmentsreporting/underlyingdata/xpfund_performance_quality"
        self._summary_data_location = "raw/investmentsreporting/summarydata/xpfund_performance_quality"

    @cached_property
    def _constituents(self):
        return self._get_peer_constituents()

    @cached_property
    def _constituent_returns(self):
        inv_group_ids = self._constituents['InvestmentGroupId'].unique().tolist()
        return self._get_constituent_returns(investment_group_ids=inv_group_ids)

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
        #TODO NEED TO REPLACE WITH ACTUAL
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

    def _get_peer_constituents(self):
        return self._strategy_benchmark.get_altsoft_peer_constituents(peer_names=self._peer_group)

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

    def _get_annualized_return(self):
        ror = self._analytics.compute_trailing_return(
            ror=self._constituent_returns,
            window=self._trailing_months,
            as_of_date=self._as_of_date,
            method="geometric",
            periodicity=Periodicity.Monthly,
            annualize=True,
        )
        ror = ror.to_frame('Return')
        return ror

    def _get_annualized_vol(self):
        vol = self._analytics.compute_trailing_vol(
            ror=self._constituent_returns,
            window=self._trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
            annualize=True,
        )
        vol = vol.to_frame('Vol')
        return vol

    def _get_sharpe_ratio(self):
        sharpe = self._analytics.compute_trailing_sharpe_ratio(
            ror=self._constituent_returns,
            rf_ror=self._rf_returns,
            window=self._trailing_months,
            as_of_date=self._as_of_date,
            periodicity=Periodicity.Monthly,
        )
        sharpe = sharpe.to_frame('Sharpe')
        return sharpe

    def _get_return_in_peer_stress_months(self):
        avg_peer_return = self._constituent_returns.median(axis=1)
        number_months = round(self._trailing_months * 0.10)
        worst_peer_months = avg_peer_return.sort_values()[0:number_months]
        worst_peer_dates = worst_peer_months.index.tolist()
        peer_stress_ror = self._constituent_returns.loc[worst_peer_dates].mean(axis=0)
        peer_stress_ror = peer_stress_ror.to_frame('PeerStressRor')
        return peer_stress_ror

    def _get_max_1mo_return(self):
        max_ror = self._constituent_returns.max(axis=0)
        max_ror = max_ror.to_frame('MaxRor')
        return max_ror

    @staticmethod
    def _calculate_peer_rankings(standalone_metrics, relative_metrics):
        #TODO replace with Daivik ranking
        rankings = standalone_metrics.merge(relative_metrics, left_index=True, right_index=True)
        rankings = rankings.reindex(['Sharpe', 'PeerStressRor', 'MaxRor', 'Excess', 'R2'], axis=1)
        # low R2 is better
        rankings['R2'] = -rankings['R2']
        #rankings = rankings.apply(lambda x: (x - x.mean()) / x.std())
        rankings = rankings.dropna()
        rankings = rankings.rank(axis=0, ascending=True)
        rankings = rankings.mean(axis=1).sort_values(ascending=True)
        rankings = pd.qcut(x=rankings, q=[0, 0.25, 0.50, 0.75, 1], labels=[4, 3, 2, 1])
        rankings = rankings.reset_index().rename(columns={'index': 'InvestmentGroupName', 0: 'Quartile'})
        rankings = rankings.sort_values(['Quartile', 'InvestmentGroupName'], ascending=[False, True])
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
        def _get_peers_with_returns_in_ttm(returns):
            return returns.notna()[-12:].any().sum()

        def _get_peers_with_current_month_return(returns):
            return returns.notna().sum(axis=1)[-1]

        def _summarize_counts(returns):
            if returns.shape[0] == 0:
                return [np.nan, np.nan]

            updated_constituents = _get_peers_with_current_month_return(returns)
            active_constituents = _get_peers_with_returns_in_ttm(returns)
            return [updated_constituents, active_constituents]

        counts = _summarize_counts(returns=self._constituent_returns)

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

        return summary

    def _get_arb_excess_return(self):
        fund_return = self._get_annualized_return()
        fund_return.rename(columns={"Return": "Fund"}, inplace=True)

        bmrk_return = self._analytics.compute_trailing_return(
            ror=self._absolute_benchmark_returns,
            window=self._trailing_months,
            as_of_date=self._as_of_date,
            method="geometric",
            periodicity=Periodicity.Monthly,
            annualize=True,
        )
        bmrk_return = bmrk_return.to_frame('Bmrk')

        fund_bmrk_return = fund_return.merge(bmrk_return, left_index=True, right_index=True)
        fund_bmrk_return['Excess'] = fund_bmrk_return['Fund'] - fund_bmrk_return['Bmrk']
        excess_return = fund_bmrk_return['Excess'].to_frame('Excess')
        excess_return['Excess'] = excess_return['Excess'].round(2)
        return excess_return

    def _get_arb_r_squared(self):
        funds = self._constituent_returns.columns.tolist()
        r2_summary = pd.DataFrame(index=funds)
        for fund in funds:
            fund_return = self._constituent_returns[fund].to_frame('Fund')
            fund_return = fund_return.astype(float)
            if fund in self._absolute_benchmark_returns.columns:
                bmrk_return = self._absolute_benchmark_returns[fund].to_frame('Bmrk')
                data = fund_return.merge(bmrk_return, left_index=True, right_index=True)
                correlation = data.corr(min_periods=self._trailing_months)
                r2 = correlation.loc['Fund', 'Bmrk'] ** 2
                r2 = r2.round(2)
                r2_summary.loc[fund, 'R2'] = r2
        return r2_summary

    def build_absolute_return_benchmark_summary(self):
        excess_return = self._get_arb_excess_return()
        r_squared = self._get_arb_r_squared()
        summary = excess_return.merge(r_squared, how='left', left_index=True, right_index=True)
        return summary

    def build_rba_excess_return_summary(self):
        inv_group_ids = self._constituents['InvestmentGroupId'].unique().tolist()
        inv_group = InvestmentGroup(investment_group_ids=inv_group_ids)
        rba = inv_group.get_rba_return_decomposition_by_date(
            start_date=self._start_date, end_date=self._end_date, frequency='M', window=36,
            factor_filter=['NON_FACTOR'])
        excess = self._analytics.compute_trailing_return(
            ror=rba,
            window=self._trailing_months,
            as_of_date=self._as_of_date,
            method="geometric",
            periodicity=Periodicity.Monthly,
            annualize=True,
        )
        excess = excess.reset_index()
        excess = excess[excess['AggLevel'] == 'NON_FACTOR']
        excess.columns = excess.columns[:-1].tolist() + ['Excess']
        excess.drop(columns={'InvestmentGroupId', 'FactorGroup1'}, inplace=True)
        excess = excess[excess['AggLevel'] == 'NON_FACTOR'].drop(columns='AggLevel')
        excess = excess.set_index('InvestmentGroupName')
        excess.rename(columns={'Excess': 'RbaAlpha'}, inplace=True)

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

    @staticmethod
    def build_qtile_summary(rankings, summary, qtile):
        fund_names = rankings[rankings['Quartile'] == qtile]
        fund_names = pd.DataFrame(index=fund_names['InvestmentGroupName'].values)

        summary = fund_names.merge(summary, left_index=True, right_index=True)
        rows_to_pad = 75 - len(summary)
        summary = summary.reindex(summary.index.tolist() + [''] * rows_to_pad)
        summary_table = summary.reset_index()
        return summary_table

    def build_omitted_fund_summary(self):
        short_track_index = self._constituent_returns.count(axis=0) < 36
        short_track_funds = short_track_index[short_track_index].index.tolist()

        missing_mtd_index = self._constituent_returns.iloc[-1].isna()
        missing_mtd_funds = missing_mtd_index[missing_mtd_index].index.tolist()

        omitted_funds = sorted(list(set(short_track_funds) | set(missing_mtd_funds)))

        omitted_fund_returns = self._constituent_returns.loc[:, omitted_funds]

        last_return_date = omitted_fund_returns.apply(lambda x: x.last_valid_index())
        last_return_date = last_return_date.to_frame('LastReturnDate')

        return_count = omitted_fund_returns.count(axis=0)
        return_count = return_count.to_frame('NoReturns')

        #TODO adjust
        partial_return = omitted_fund_returns.mean() * 12
        partial_return = partial_return.to_frame('PartialReturn')

        summary = last_return_date.merge(return_count, left_index=True, right_index=True)
        summary = summary.merge(partial_return, left_index=True, right_index=True)

        rows_to_pad = 75 - len(summary)
        summary = summary.reindex(summary.index.tolist() + [''] * rows_to_pad)
        summary = summary.reset_index()
        return summary

    def generate_peer_ranking_report(self):
        # if not self._validate_inputs():
        #     return 'Invalid inputs'

        logging.info("Generating report for: " + self._peer_group)

        header_info = self.get_header_info()
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

        first_qtile = self.build_qtile_summary(rankings=rankings, summary=summary, qtile=1)
        second_qtile = self.build_qtile_summary(rankings=rankings, summary=summary, qtile=2)
        third_qtile = self.build_qtile_summary(rankings=rankings, summary=summary, qtile=3)
        fourth_qtile = self.build_qtile_summary(rankings=rankings, summary=summary, qtile=4)

        omitted_funds = self.build_omitted_fund_summary()

        logging.info('Report summary data generated for: ' + self._peer_group)

        input_data = {
            "header_info1": header_info,
            "header_info2": header_info,
            "header_info3": header_info,
            "header_info4": header_info,
            "header_info5": header_info,
            "first_qtile": first_qtile,
            "second_qtile": second_qtile,
            "third_qtile": third_qtile,
            "fourth_qtile": fourth_qtile,
            "omitted_funds": omitted_funds,
            "constituents1": constituent_counts,
            "constituents2": constituent_counts,
            "constituents3": constituent_counts,
            "constituents4": constituent_counts,
            "constituents5": constituent_counts,
        }

        input_data_json = {
            "header_info1": header_info.to_json(orient='index'),
            "header_info2": header_info.to_json(orient='index'),
            "header_info3": header_info.to_json(orient='index'),
            "header_info4": header_info.to_json(orient='index'),
            "header_info5": header_info.to_json(orient='index'),
            "first_qtile": first_qtile.to_json(orient='index'),
            "second_qtile": second_qtile.to_json(orient='index'),
            "third_qtile": third_qtile.to_json(orient='index'),
            "fourth_qtile": fourth_qtile.to_json(orient='index'),
            "omitted_funds": omitted_funds.to_json(orient='index'),
            "constituents1": constituent_counts.to_json(orient='index'),
            "constituents2": constituent_counts.to_json(orient='index'),
            "constituents3": constituent_counts.to_json(orient='index'),
            "constituents4": constituent_counts.to_json(orient='index'),
            "constituents5": constituent_counts.to_json(orient='index'),
        }

        data_to_write = json.dumps(input_data_json)
        asofdate = self._as_of_date.strftime("%Y-%m-%d")
        write_params = AzureDataLakeDao.create_get_data_params(
            self._summary_data_location,
            self._peer_group.replace("/", "") + asofdate + ".json",
            retry=False,
        )
        self._runner.execute(
            params=write_params,
            source=DaoSource.DataLake,
            operation=lambda dao, params: dao.post_data(params, data_to_write),
        )

        logging.info("JSON stored to DataLake for: " + self._peer_group)

        as_of_date = dt.datetime.combine(self._as_of_date, dt.datetime.min.time())
        with Scenario(asofdate=as_of_date).context():
            InvestmentsReportRunner().execute(
                data=input_data,
                template="XPFUND_PerformanceQuality_Template.xlsx",
                save=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.cross_entity,
                entity_name=self._peer_group,
                entity_display_name=self._peer_group.replace("/", ""),
                # entity_ids=[self._pub_investment_group_id.item()],
                # entity_source=DaoSource.PubDwh,
                report_name="XPFUND Performance Quality",
                report_type=ReportType.Performance,
                report_vertical=ReportVertical.ARS,
                report_frequency="Monthly",
                aggregate_intervals=AggregateInterval.MTD,
                # output_dir="cleansed/investmentsreporting/printedexcels/",
                # report_output_source=DaoSource.DataLake,
            )

        logging.info("Excel stored to DataLake for: " + self._peer_group)

    def run(self, **kwargs):
        self.generate_peer_ranking_report()
        return True
