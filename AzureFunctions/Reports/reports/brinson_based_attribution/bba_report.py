import pandas as pd
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs
from gcm.Dao.DaoSources import DaoSource
from gcm.Dao.Utils.bulk_insert.sql_bulk_insert import SqlBulkInsert
from gcm.inv.dataprovider.entity_master import EntityMaster
from gcm.inv.dataprovider.portfolio import Portfolio
from gcm.inv.dataprovider.strategy_benchmark import StrategyBenchmark
from sqlalchemy import func, or_
import datetime as dt
import numpy as np
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import MonthEnd
import statsmodels.api as sm
from gcm.inv.dataprovider.investment_group import InvestmentGroup
from gcm.inv.dataprovider.portfolio import Portfolio
from gcm.inv.dataprovider.strategy_benchmark import StrategyBenchmark


from gcm.inv.reporting.core.Runners.investmentsreporting import (
    InvestmentsReportRunner,
)
from gcm.inv.reporting.core.reporting_runner_base import (
    ReportingRunnerBase,
)
from gcm.Scenario.scenario import Scenario


class BbaReport():
    def __init__(self, runner, report_date):
        # super().__init__(runner=Scenario.get_attribute("runner"))
        self._runner = runner
        self._report_date = report_date
        # DT: self._eh is ran twice below; no good reason, should be refactored
        self._portfolio = Portfolio(runner=self._runner)
        self._strategy_benchmark = StrategyBenchmark(runner=runner)
        self._investment_group = InvestmentGroup(runner=self._runner)
        self._entity_master = EntityMaster(runner=self._runner)
        self._gcm, self._eh = self.get_allocation_data_portfolio(end_date=self._report_date)
        self._gcm_firmwide, self._eh = self.get_allocation_data_firmwide()
        # self._wb = load_workbook('C:\Code\Gcm.RiskScripts\Analyses\GCM_ARS_vs_Market_Analysis\ReportTemplate.xlsx')
        # self._excel_io = ExcelIO()
        self._output_dir = 'C:\Code\Gcm.RiskScripts\Analyses\GCM_ARS_vs_Market_Analysis\Output\\'
        self._strategy_order = ['Credit', 'Long/Short Equity', 'Macro', 'Multi-Strategy', 'Quantitative',
                                'Relative Value']
        self._trailing_periods = [1, self._report_date.month - dt.date(self._report_date.year,
                                                                       3 * ((self._report_date.month - 1) // 3) + 1,
                                                                       1).month + 1,
                                  self._report_date.month, 12, 36, 60, 120]
        self._trailing_period_df = pd.DataFrame({'Period': ['MTD', 'QTD', 'YTD', 'TTM', '3Y', '5Y', '10Y'],
                                                 'TrailingPeriod': [1, self._report_date.month - dt.date(
                                                     self._report_date.year, 3 * (
                                                             (self._report_date.month - 1) // 3) + 1, 1).month + 1,
                                                                    self._report_date.month, 12, 36, 60, 120]
                                                 })
        self._period_order = ['MTD', 'QTD', 'YTD', 'TTM', '3Y', '5Y', '10Y']

    def get_excess_return_rpt(self, port_rtn, bmark_rtn):
        ytd_excess = self.get_excess_return_stats(dt.date(self._report_date.year, 1, 1), port_rtn, bmark_rtn).rename(
            columns={'value': 'YTD'})
        ttm_excess = self.get_excess_return_stats(self._report_date - relativedelta(years=1) + relativedelta(months=1),
                                                  port_rtn,
                                                  bmark_rtn).rename(columns={'value': 'TTM'})
        threey_excess = self.get_excess_return_stats(
            self._report_date - relativedelta(years=3) + relativedelta(months=1),
            port_rtn,
            bmark_rtn).rename(columns={'value': '3Y'})
        # dumb way to annualize but get it out the door
        threey_excess.loc[['CtrTotal', 'CtrContrib_Outperformer', 'CtrContrib_Underperformer',
                           'AvgExcess_Outperformer', 'AvgExcess_Underperformer']] = (1 + threey_excess.loc[
            ['CtrTotal', 'CtrContrib_Outperformer',
             'CtrContrib_Underperformer', 'AvgExcess_Outperformer',
             'AvgExcess_Underperformer']]) ** (1 / 3) - 1

        fivey_excess = self.get_excess_return_stats(
            self._report_date - relativedelta(years=5) + relativedelta(months=1),
            port_rtn,
            bmark_rtn).rename(columns={'value': '5Y'})

        # dumb way to annualize but get it out the door
        fivey_excess.loc[['CtrTotal', 'CtrContrib_Outperformer', 'CtrContrib_Underperformer',
                          'AvgExcess_Outperformer', 'AvgExcess_Underperformer']] = (1 + fivey_excess.loc[
            ['CtrTotal', 'CtrContrib_Outperformer',
             'CtrContrib_Underperformer', 'AvgExcess_Outperformer',
             'AvgExcess_Underperformer']]) ** (1 / 5) - 1

        result = ytd_excess.merge(ttm_excess, how='left', left_index=True, right_index=True).merge(
            threey_excess, how='left', left_index=True, right_index=True).merge(
            fivey_excess, how='left', left_index=True, right_index=True)[['YTD', 'TTM', '3Y', '5Y']]
        return result

    def get_excess_return_stats(self, start_date, port_rtn, bmark_rtn):
        port_rtn = port_rtn[(port_rtn.Date >= start_date) & (port_rtn.Date <= self._report_date)]
        port_rtn.Date = port_rtn.Date + MonthEnd(1)
        return_df = port_rtn[['Date', 'InvestmentGroupName', 'pct_investment_of_portfolio_total', 'Ror']].merge(
            bmark_rtn.reset_index(), how='left', left_on='Date', right_on='Date')
        assert (len(port_rtn) == len(return_df))

        return_df['Excess'] = return_df.Ror - return_df.EHI200

        funds_classified = return_df[['InvestmentGroupName', 'Excess']].groupby(
            ['InvestmentGroupName']).sum().reset_index()
        funds_classified['Type'] = np.where(funds_classified.Excess > 0, 'Outperformer', 'Underperformer')

        port_classified = return_df.merge(funds_classified[['InvestmentGroupName', 'Type']], how='left',
                                          left_on='InvestmentGroupName',
                                          right_on='InvestmentGroupName')
        assert (len(port_classified) == len(port_rtn))
        assert (len(port_classified) == len(return_df))

        port_classified['Ctr'] = port_classified.Excess * port_classified.pct_investment_of_portfolio_total
        ctr = self.calc_ctr(
            port_classified.pivot_table(index='Date', columns='InvestmentGroupName', values='Ctr').fillna(
                0)).reset_index()
        ctr_classified = ctr.merge(funds_classified, how='left', left_on='InvestmentGroupName',
                                   right_on='InvestmentGroupName')

        ctr_contrib = ctr_classified[['Type', 'CTR']].groupby('Type').sum().reindex(['Outperformer', 'Underperformer'])
        ctr_total = ctr_classified.CTR.sum()

        hit_rate = ctr_classified[['Type', 'InvestmentGroupName']].groupby('Type').count().reindex(
            ['Outperformer', 'Underperformer'])
        hit_rate['pct'] = hit_rate / hit_rate.sum()

        excess_return_series = return_df.pivot_table(index='Date', columns='InvestmentGroupName', values='Excess')
        inv_stats = pd.DataFrame()
        for i in excess_return_series.columns:
            # if return_df[[i]][return_df.index==self._report_date].isnull().squeeze():
            #     continue
            inv_stats = pd.concat([inv_stats, self.ann_return(
                excess_return_series[[i]], trailing_periods=['Incep'], freq=12, return_NoObs=True)])

        inv_stats_classified = inv_stats.merge(funds_classified, how='left', left_on='InvestmentGroupName',
                                               right_on='InvestmentGroupName')
        avg_excess = inv_stats_classified[['Type', 'AnnRor']].groupby('Type').mean().reindex(
            ['Outperformer', 'Underperformer'])
        avg_excess_ratio = avg_excess[avg_excess.index == 'Outperformer'].AnnRor.abs().squeeze() / avg_excess[
            avg_excess.index == 'Underperformer'].AnnRor.abs().squeeze()

        avg_size = port_classified[['Type', 'pct_investment_of_portfolio_total']].groupby('Type').mean().reindex(
            ['Outperformer', 'Underperformer'])
        avg_size_ratio = avg_size[avg_size.index == 'Outperformer'].pct_investment_of_portfolio_total.abs().squeeze() / \
                         avg_size[avg_size.index == 'Underperformer'].pct_investment_of_portfolio_total.abs().squeeze()

        totals = pd.DataFrame({'Name': ['CtrTotal', 'AvgExcessRatio', 'AvgSizeRatio'],
                               'value': [ctr_total, avg_excess_ratio, avg_size_ratio]})
        breakdowns = ctr_contrib.merge(avg_excess, how='left', left_index=True, right_index=True).merge(
            avg_size, how='left', left_index=True, right_index=True).merge(
            hit_rate[['pct']], how='left', left_index=True, right_index=True).rename(
            columns={'CTR': 'CtrContrib', 'AnnRor': 'AvgExcess', 'pct_investment_of_portfolio_total': 'AvgSize',
                     'pct': 'HitRate'}
        )
        breakdowns = pd.melt(breakdowns.reset_index(), id_vars=['Type'])
        breakdowns['Name'] = breakdowns.variable.astype('str') + '_' + breakdowns.Type.astype('str')

        result = pd.concat([totals, breakdowns[['Name', 'value']]]).set_index('Name').reindex(
            ['CtrTotal', 'CtrContrib_Outperformer', 'CtrContrib_Underperformer',
             'HitRate_Outperformer', 'HitRate_Underperformer', 'Blank1', 'AvgExcessRatio', 'AvgExcess_Outperformer',
             'AvgExcess_Underperformer',
             'Blank2', 'AvgSizeRatio', 'AvgSize_Outperformer', 'AvgSize_Underperformer'])

        return result
        # uncomment this if we want to determine over/under performers by ann ROR - instead using sum of monthlies
        # bmark_stats = self.ann_return(
        #     bmark_rtn, trailing_periods=self._trailing_periods, freq=12, return_NoObs=True)
        # trailing_periods = self._trailing_period_df[self._trailing_period_df.Period.isin(['YTD', 'TTM', '3Y', '5Y'])].TrailingPeriod.to_list()
        # return_df = port_rtn.pivot_table(index='Date', columns='InvestmentGroupName', values='Ror').loc[:self._report_date]
        # port_stats = pd.DataFrame()
        # for i in return_df.columns:
        #     if return_df[[i]][return_df.index==self._report_date].isnull().squeeze():
        #         continue
        #     port_stats = pd.concat([port_stats, self.ann_return(
        #         return_df[[i]], trailing_periods=trailing_periods, freq=12, return_NoObs=True)])
        #
        # stats_merged = port_stats[['InvestmentGroupName', 'AnnRor', 'NoObs']].rename(columns={'AnnRor': 'GcmRor'}).merge(bmark_stats.rename(columns={'AnnRor': 'BmarkRor'}), how='left', left_on='NoObs', right_on='NoObs').merge(
        #     self._trailing_period_df, how='left', left_on='NoObs',
        #     right_on='TrailingPeriod').drop_duplicates()[['InvestmentGroupName', 'GcmRor', 'BmarkRor', 'Period']]
        # stats_merged['Type'] = np.where(stats_merged.GcmRor > stats_merged.BmarkRor, 'Outperformer', 'Underperformer')
        #
        # port_rtn.stats_merged.merge()

    def get_sharpe_rpt(self, port_rtn, bmark_rtn):
        bmark_stats = self.calc_sharpe(
            bmark_rtn, trailing_periods=self._trailing_periods)

        port_stats = self.calc_sharpe(
            port_rtn, trailing_periods=self._trailing_periods)

        sharpe_df = bmark_stats.merge(port_stats, how='left', left_on='NoObs', right_on='NoObs').merge(
            self._trailing_period_df, how='left', left_on='NoObs',
            right_on='TrailingPeriod').drop_duplicates().rename(columns={'Sharpe_x': 'EHI200',
                                                                         'Sharpe_y': 'GCM'})

        return sharpe_df[['Period', 'GCM', 'EHI200']].set_index('Period').reindex(self._period_order)

    def calc_sharpe(self, returns, trailing_periods):
        rf = self.get_fin_index_ror_by_name(tickers=['SBMMTB3']).rename(columns={'Ror': 'rf'})[['rf']]
        returns = returns.dropna()

        result = pd.DataFrame()
        for trailing_period in trailing_periods:
            if trailing_period < 12:
                continue
            if trailing_period != "Incep":
                if len(returns) < trailing_period:
                    continue
                else:
                    return_sub = returns.tail(trailing_period)
            else:
                return_sub = returns
            rf_ror = self.ann_return(rf, [trailing_period], freq=12)
            asset_ror = self.ann_return(return_sub, [trailing_period], freq=12)
            asset_vol = return_sub.std() * np.sqrt(12)

            sharpe_df = pd.DataFrame(
                {'Sharpe': [(asset_ror.AnnRor.squeeze() - rf_ror.AnnRor.squeeze()) / asset_vol.squeeze()],
                 'NoObs': trailing_period})
            result = result.append(sharpe_df)
        return result

    def ann_vol(self, returns, trailing_periods, freq=250):
        result = pd.DataFrame()
        for trailing_period in trailing_periods:
            if trailing_period != "Incep":
                if len(returns) < trailing_period:
                    continue
                else:
                    return_sub = returns.tail(trailing_period)
            else:
                return_sub = returns
            ann_vol = pd.DataFrame(
                return_sub.std() * np.sqrt(freq)
            )
            ann_vol["NoObs"] = len(return_sub)
            result = result.append(ann_vol)
        result = result.reset_index().rename(
            columns={"index": "Name", 0: "AnnVol"}
        )

        return result

    def get_vol_rpt(self, port_rtn, bmark_rtn):
        bmark_stats = self.ann_vol(
            bmark_rtn, trailing_periods=self._trailing_periods, freq=12)

        port_stats = self.ann_vol(
            port_rtn, trailing_periods=self._trailing_periods, freq=12)

        vol_df = bmark_stats.merge(port_stats, how='left', left_on='NoObs', right_on='NoObs').merge(
            self._trailing_period_df, how='left', left_on='NoObs',
            right_on='TrailingPeriod').drop_duplicates().rename(columns={'AnnVol_x': 'EHI200',
                                                                         'AnnVol_y': 'GCM'})

        return vol_df[['Period', 'GCM', 'EHI200']].set_index('Period').reindex(self._period_order)

    def get_downside_rpt(self, port_rtn, bmark_rtn, assets, benchmarks):
        spx = self.get_fin_index_ror_by_name(tickers=['SPXT']).rename(columns={'Ror': 'SPXT'})
        returns = spx.merge(bmark_rtn, how='left', left_index=True, right_index=True).merge(port_rtn, how='left',
                                                                                            left_index=True,
                                                                                            right_index=True)

        downside_capture = self.calculate_downside_capture(returns, assets, benchmarks, self._trailing_periods)
        downside_capture_df = downside_capture.merge(self._trailing_period_df, how='left', left_on='NoObs',
                                                     right_on='TrailingPeriod').drop_duplicates()
        result = downside_capture_df[['Asset', 'DownsideCapture', 'Period']].pivot(index='Period', columns='Asset',
                                                                                   values='DownsideCapture')[assets]
        return result.reindex(self._period_order)

    def get_correlation_rpt(self, port_rtn, bmark_rtn, assets, benchmarks):
        spx = self.get_fin_index_ror_by_name(tickers=['SPXT']).rename(columns={'Ror': 'SPXT'})
        returns = spx.merge(bmark_rtn, how='left', left_index=True, right_index=True).merge(port_rtn, how='left',
                                                                                            left_index=True,
                                                                                            right_index=True)

        correlation = self.calculate_correlation(returns, assets, benchmarks, self._trailing_periods)
        correlation_df = correlation.merge(self._trailing_period_df, how='left', left_on='NoObs',
                                           right_on='TrailingPeriod').drop_duplicates()
        result = \
        correlation_df[['Asset', 'Correlation', 'Period']].pivot(index='Period', columns='Asset', values='Correlation')[
            assets]
        return result.reindex(self._period_order)

    def calculate_correlation(self, returns, assets, benchmarks, trailing_periods):
        result = pd.DataFrame()
        for asset in assets:
            for bmk in benchmarks:
                rtns = returns[[asset, bmk]].dropna()
                for trailing_period in trailing_periods:
                    if trailing_period < 4:
                        continue
                    if trailing_period != "Incep":
                        if len(rtns) < trailing_period:
                            continue
                        else:
                            return_sub = rtns.tail(trailing_period)
                    else:
                        return_sub = rtns
                    corr_df = pd.DataFrame({'Asset': [asset],
                                            'Correlation': [return_sub.corr()[asset].loc[bmk]],
                                            'NoObs': [trailing_period]})
                    result = result.append(corr_df)
        return result

    def calculate_downside_capture(self, returns, assets, benchmarks, trailing_periods):
        result = pd.DataFrame()
        for asset in assets:
            for bmk in benchmarks:
                rtns = returns[[asset, bmk]].dropna()
                for trailing_period in trailing_periods:
                    if trailing_period != "Incep":
                        if len(rtns) < trailing_period:
                            continue
                        else:
                            return_sub = rtns.tail(trailing_period)
                    else:
                        return_sub = rtns
                    rtns_neg_bmark = return_sub[return_sub[bmk] < 0]
                    downside_df = pd.DataFrame({'Asset': [asset],
                                                'DownsideCapture': [
                                                    rtns_neg_bmark[asset].mean() / rtns_neg_bmark[bmk].mean()],
                                                'NoObs': [trailing_period]})
                    result = result.append(downside_df)
        return result

    def get_betas_rpt(self, port_rtn, bmark_rtn, assets, benchmarks):
        spx = self.get_fin_index_ror_by_name(tickers=['SPXT']).rename(columns={'Ror': 'SPXT'})
        returns = spx.merge(bmark_rtn, how='left', left_index=True, right_index=True).merge(port_rtn, how='left',
                                                                                            left_index=True,
                                                                                            right_index=True)

        betas = self.calculate_betas(returns, assets, benchmarks, self._trailing_periods)
        betas_df = betas.merge(self._trailing_period_df, how='left', left_on='NoObs',
                               right_on='TrailingPeriod').drop_duplicates()

        result = betas_df[['Name', 'Beta', 'Period']].pivot(index='Period', columns='Name', values='Beta')[assets]
        return result.reindex(self._period_order)

    def calculate_betas(self, returns, assets, benchmarks, trailing_periods):
        returns["intercept"] = 1

        result = pd.DataFrame()
        for asset in assets:
            for bmk in benchmarks:
                rtns = returns[[asset, bmk, 'intercept']].dropna()
                for trailing_period in trailing_periods:
                    if trailing_period != "Incep":
                        if trailing_period < 4:
                            continue
                        if len(rtns) < trailing_period:
                            continue
                        else:
                            return_sub = rtns.tail(trailing_period)
                    else:
                        return_sub = rtns
                    return_trimmed = return_sub.dropna()
                    fit = sm.OLS(
                        return_trimmed[asset],
                        return_trimmed[[bmk, "intercept"]],
                    ).fit()
                    alpha_df = pd.DataFrame(
                        [
                            [
                                asset,
                                bmk,
                                fit.params[bmk],
                                fit.params.intercept * 12,
                                fit.rsquared_adj,
                                len(return_sub),
                            ]
                        ],
                        columns=[
                            "Name",
                            "Benchmark",
                            "Beta",
                            "AnnAlpha",
                            "AdjR2",
                            "NoObs",
                        ],
                    )
                    result = result.append(alpha_df)
        result = result.loc[
            (result.Name != "intercept") & (result.Benchmark != "intercept")
            ]
        return result

    def get_returns_rpt(self, port_rtn, bmark_rtn):
        bmark_stats = self.ann_return(
            bmark_rtn, trailing_periods=self._trailing_periods, freq=12, return_NoObs=True)

        port_stats = self.ann_return(
            port_rtn, trailing_periods=self._trailing_periods, freq=12, return_NoObs=True)

        return_df = bmark_stats.merge(port_stats, how='left', left_on='NoObs', right_on='NoObs').merge(
            self._trailing_period_df, how='left', left_on='NoObs',
            right_on='TrailingPeriod').drop_duplicates().rename(columns={'AnnRor_x': 'EHI200',
                                                                         'AnnRor_y': 'GCM'})
        return_df['excess'] = return_df.GCM - return_df.EHI200
        return_df.excess = np.where(return_df.GCM.isnull(), None, return_df.excess)

        return return_df[['Period', 'GCM', 'EHI200', 'excess']].set_index('Period').reindex(self._period_order)

    def get_fin_index_ror_by_name(self, tickers):
        p = {
            "table": "FinancialIndexReturnFact",
            "schema": "reporting",
            "operation": lambda query, item: query.filter(
                item.BloombergTicker.in_(tickers)
            ),
        }
        df = self._runner.execute(
            params=p,
            source=DaoSource.PubDwh,
            operation=lambda dao, params: dao.get_data(params),
        )
        df = df[["PeriodDate", "RateOfReturn", "BloombergTicker"]].rename(
            columns={
                "PeriodDate": "Date",
                "RateOfReturn": "Ror",
                "BloombergTicker": "Ticker",
            }
        ).set_index('Date')
        df.index = df.index + MonthEnd(1)

        return df

    # TODO: refactor to get_eurekahedge_benchmark_returns
    def get_ehi_returns(self):
        ret = self._runner.execute(
            params={
                "schema": "eurekahedge",
                "table": "IndexReturnFact",
                "operation": lambda query, item: query.with_entities(
                    item.Date,
                    item.Ror).filter(
                    item.IndexName == 'Eurekahedge Institutional 200'
                ),
            },
            source=DaoSource.InvestmentsDwh,
            operation=lambda dao, params: dao.get_data(params),
        )
        ret['Date'] = pd.to_datetime(ret['Date'])
        ret = ret.set_index('Date').sort_index().rename(columns={'Ror': 'EHI200'})
        ret.index = ret.index + MonthEnd(1)
        return ret

    def get_portfolio_ret(self, acronym, start_date="1900-01-01", end_date='2099-01-01'):

        ret = self._runner.execute(
            params={
                "schema": "analytics",
                "table": "PortfolioReturns",
                "operation": lambda query, item: query.with_entities(
                    item.PeriodDate,
                    item.Acronym,
                    item.RateOfReturn).filter(
                    item.Acronym.in_(acronym),
                    item.IsMonthEnd == '1',
                    item.PeriodDate >= start_date,
                    item.PeriodDate <= end_date
                ),
            },
            source=DaoSource.PubDwh,
            operation=lambda dao, params: dao.get_data(params),
        )

        ret['PeriodDate'] = pd.to_datetime(ret['PeriodDate'])
        ret = ret.pivot(index='PeriodDate', columns='Acronym', values='RateOfReturn').sort_index()

        return ret

    def get_portfolio_bba_outliers(self, acronyms, gcm, bmark, start_date, end_date, trailing_period):
        df = self.get_strategy_sizing_outliers(acronyms, gcm, bmark, start_date, end_date, trailing_period)
        top_10_strategy_sizing = self.get_top_n_portfolios(df=df, col_name='StrategySizing', n=10, asc=False)
        bottom_10_strategy_sizing = self.get_top_n_portfolios(df=df, col_name='StrategySizing', n=10, asc=True)
        top_10_manager_sizing = self.get_top_n_portfolios(df=df, col_name='ManagerSizing', n=10, asc=False)
        bottom_10_manager_sizing = self.get_top_n_portfolios(df=df, col_name='ManagerSizing', n=10, asc=True)
        top_10_manager_selection = self.get_top_n_portfolios(df=df, col_name='ManagerSelection', n=10, asc=False)
        bottom_10_manager_selection = self.get_top_n_portfolios(df=df, col_name='ManagerSelection', n=10, asc=True)
        top_10_strategy_selection = self.get_top_n_portfolios(df=df, col_name='StrategySelection', n=10, asc=False)
        bottom_10_strategy_selection = self.get_top_n_portfolios(df=df, col_name='StrategySelection', n=10, asc=True)
        return top_10_strategy_sizing, bottom_10_strategy_sizing, top_10_manager_sizing, bottom_10_manager_sizing, top_10_manager_selection, bottom_10_manager_selection, top_10_strategy_selection, bottom_10_strategy_selection

    def get_top_n_portfolios(self, df, col_name, n, asc):
        result = df.sort_values(col_name, ascending=asc).head(n)
        return result[['Acronym', col_name]]

    def get_strategy_sizing_outliers(self, acronyms, gcm, bmark, start_date, end_date, trailing_period):
        error_df = pd.DataFrame()
        attribution_by_portfolio = pd.DataFrame()

        remove_acronyms_not_multistrat_df = gcm[['Date', 'Acronym', 'pct_strategy_of_portfolio_total']][
            (gcm.Date >= start_date) & (gcm.Date <= end_date)].drop_duplicates(). \
            groupby(['Date', 'Acronym']).count().groupby('Acronym').mean().reset_index()
        remove_acronyms_not_multistrat = remove_acronyms_not_multistrat_df[
            remove_acronyms_not_multistrat_df.pct_strategy_of_portfolio_total < 2].Acronym.to_list()
        remove_tickers_aum_df = gcm[gcm.Date == end_date][['Acronym', 'TotalDollarPortfolio']].drop_duplicates()
        remove_tickers_aum = remove_tickers_aum_df[
            remove_tickers_aum_df.TotalDollarPortfolio < 100000000].Acronym.to_list()
        acronyms_to_run = list(
            filter(lambda x: x not in remove_acronyms_not_multistrat and x not in remove_tickers_aum, acronyms))
        for acronym in acronyms_to_run:
            print(acronym)
            try:
                gcm_df = gcm[gcm.Acronym == acronym]
                portfolio_attrib = self.get_attribution_rpt(gcm_df, bmark, start_date, end_date, trailing_period)
                portfolio_attrib['Acronym'] = acronym

                # by strat, will be summed to total portfolio
                attribution_by_portfolio = pd.concat([attribution_by_portfolio, portfolio_attrib])

            except Exception as e:
                error_msg = getattr(e, "message", repr(e))
                print(error_msg)
                error_df = pd.concat([pd.DataFrame(
                    {
                        "Portfolio": [acronym],
                        "Date": [end_date],
                        "ErrorMessage": [error_msg],
                    }
                ), error_df])

        result = attribution_by_portfolio[
            ['Acronym', 'StrategySizing', 'ManagerSelection', 'StrategySelection', 'ManagerSizing']].fillna(0). \
            groupby('Acronym').sum().reset_index()
        return result

    def get_ars_portfolio_return(self):
        pass

    def get_attribution_rpt(self, gcm, bmark, start_date, end_date, trailing_period):
        gcm_alloc = gcm[['Date', 'Strategy', 'pct_strategy_of_portfolio_total']].rename(
            columns={'pct_strategy_of_portfolio_total': 'GcmAllocation'}) \
            .drop_duplicates()
        bmark_alloc = bmark[['Date', 'Strategy', 'pct_strategy_of_total']].rename(
            columns={'pct_strategy_of_total': 'BmarkAllocation'}) \
            .drop_duplicates()
        gcm_cap_ror = self.get_returns_from_allocs(df=gcm, group_cols=['Date', 'Strategy'],
                                                   multiply_x='Ror',
                                                   multiply_y='pct_investment_of_portfolio_strategy_total')

        gcm_eq_ror = self.get_returns_from_allocs(df=gcm, group_cols=['Date', 'Strategy'],
                                                  equal_weight=True)

        bmark_eq_ror = self.get_returns_from_allocs(df=bmark, group_cols=['Date', 'Strategy'],
                                                    equal_weight=True)
        bmark_ror = self.get_returns_from_allocs(df=bmark, group_cols=['Date'],
                                                 equal_weight=True).rename(columns={'Ror': 'BmarkRor'})

        strategy_sizing = self.get_strategy_sizing(gcm_alloc, bmark_alloc, bmark_eq_ror, bmark_ror, start_date,
                                                   end_date)
        manager_sizing = self.get_manager_selection(gcm_alloc, gcm_cap_ror, gcm_eq_ror, bmark_eq_ror, start_date,
                                                    end_date)
        # strategy_selection = self.get_strategy_selection()

        result = strategy_sizing.merge(manager_sizing, left_index=True, right_index=True)
        result[result.columns] = np.where(result[result.columns] == 0, None, result[result.columns])
        return result

    def get_strategy_selection(self, gcm_alloc, gcm_cap_ror, bmark_eq_ror, start_date, end_date):
        eligible_strats_selection = gcm_alloc.pivot_table(index='Date', columns='Strategy',
                                                          values='GcmAllocation').fillna(0).loc[
                                    start_date:end_date].sum() \
            .reindex(self._strategy_order).reset_index().rename(columns={0: 'GcmAllocation'})
        eligible_strats_selection = eligible_strats_selection[(eligible_strats_selection.GcmAllocation == 0) | (
            eligible_strats_selection.GcmAllocation.isnull())].Strategy.to_list()

    def get_manager_selection(self, gcm_alloc, gcm_cap_ror, gcm_eq_ror, bmark_eq_ror, start_date, end_date):
        df_alloc_ror = gcm_alloc.merge(gcm_cap_ror.rename(columns={'Ror': 'GcmRor'}), how='left',
                                       left_on=['Date', 'Strategy'],
                                       right_on=['Date', 'Strategy']).merge(
            bmark_eq_ror.rename(columns={'Ror': 'BmarkRor'}),
            how='left', left_on=['Date', 'Strategy'],
            right_on=['Date', 'Strategy']).fillna(0)
        df_alloc_ror['Ror_less_Bmark'] = df_alloc_ror.GcmRor - df_alloc_ror.BmarkRor
        df_alloc_ror['Ctr'] = df_alloc_ror.GcmAllocation * df_alloc_ror.Ror_less_Bmark

        ctr_df = self.calc_ctr(
            df_alloc_ror.pivot_table(index='Date', columns='Strategy', values='Ctr').fillna(0) \
                .loc[start_date:end_date]).reindex(self._strategy_order).rename(columns={'CTR': 'ManagerSelection'})

        df_gcm_eq_cap = gcm_alloc.merge(gcm_cap_ror.rename(columns={'Ror': 'GcmCapRor'}), how='left',
                                        left_on=['Date', 'Strategy'],
                                        right_on=['Date', 'Strategy']).merge(
            gcm_eq_ror.rename(columns={'Ror': 'GcmEqlRor'}),
            how='left', left_on=['Date', 'Strategy'],
            right_on=['Date', 'Strategy']).fillna(0)

        df_gcm_eq_cap['Cap_less_eql'] = df_gcm_eq_cap.GcmCapRor - df_gcm_eq_cap.GcmEqlRor
        df_gcm_eq_cap['Ctr'] = df_gcm_eq_cap.GcmAllocation * df_gcm_eq_cap.Cap_less_eql

        ctr_eql_df = self.calc_ctr(
            df_gcm_eq_cap.pivot_table(index='Date', columns='Strategy', values='Ctr').fillna(0) \
                .loc[start_date:end_date]).reindex(self._strategy_order).rename(columns={'CTR': 'ManagerSizing'})

        ctr_rslt = ctr_df.merge(ctr_eql_df, how='left', left_index=True, right_index=True)
        ctr_rslt.ManagerSelection = ctr_rslt.ManagerSelection - ctr_rslt.ManagerSizing

        return ctr_rslt

    def get_strategy_sizing(self, gcm_alloc, bmark_alloc, bmark_eq_ror, bmark_ror, start_date, end_date):
        allocs = bmark_alloc.merge(gcm_alloc, how='left', left_on=['Date', 'Strategy'],
                                   right_on=['Date', 'Strategy']).fillna(0)
        allocs['Allocation'] = allocs.GcmAllocation - allocs.BmarkAllocation
        allocs['GcmMissing'] = np.where(allocs.GcmAllocation == 0, 'Selection', 'Sizing')

        returns = bmark_eq_ror.merge(bmark_ror, how='left', left_on=['Date'], right_on=['Date']).fillna(0)
        returns['Ror_less_Bmark'] = returns.Ror - returns.BmarkRor
        # use just strategy ror for bhb rather than bf formula
        # returns['Ror_less_Bmark'] = returns.Ror

        df_alloc_ror = allocs[['Date', 'Strategy', 'Allocation', 'GcmMissing']].merge(
            returns[['Date', 'Strategy', 'Ror_less_Bmark']],
            how='left', left_on=['Date', 'Strategy'], right_on=['Date', 'Strategy'])
        df_alloc_ror['Ctr'] = df_alloc_ror.Allocation * df_alloc_ror.Ror_less_Bmark

        ctr = self.calc_ctr(
            df_alloc_ror.pivot_table(index='Date', columns=['Strategy', 'GcmMissing'], values='Ctr').fillna(0) \
                .loc[start_date:end_date]).reset_index()
        ctr_df = ctr.pivot_table(index='Strategy', columns='GcmMissing', values='CTR') \
            .reindex(self._strategy_order).rename(columns={'Selection': 'StrategySelection',
                                                           'Sizing': 'StrategySizing'})

        return ctr_df

    def get_ctr_rpt(self, gcm, bmark, start_date, end_date, trailing_period):
        gcm_alloc = gcm[['Date', 'Strategy', 'pct_strategy_of_portfolio_total']].drop_duplicates().pivot(index='Date',
                                                                                                         columns='Strategy',
                                                                                                         values='pct_strategy_of_portfolio_total')
        gcm_cap_ror = self.get_returns_from_allocs(df=gcm, group_cols=['Date', 'Strategy'],
                                                   multiply_x='Ror',
                                                   multiply_y='pct_investment_of_portfolio_strategy_total')
        gcm_ror_pivot = gcm_cap_ror.pivot(index='Date', columns='Strategy', values='Ror')
        gcm_mtd_ctr = gcm_ror_pivot * gcm_alloc
        gcm_ctr_stat = self.calc_ctr(gcm_mtd_ctr.fillna(0).loc[start_date:end_date]).reindex(
            self._strategy_order)

        bmark_alloc = bmark[['Date', 'Strategy', 'pct_strategy_of_total']].drop_duplicates().pivot(index='Date',
                                                                                                   columns='Strategy',
                                                                                                   values='pct_strategy_of_total')
        bmark_eq_ror = self.get_returns_from_allocs(df=bmark, group_cols=['Date', 'Strategy'],
                                                    equal_weight=True)
        bmark_ror_pivot = bmark_eq_ror.pivot(index='Date', columns='Strategy', values='Ror')
        bmark_mtd_ctr = bmark_ror_pivot * bmark_alloc
        bmark_ctr_stat = self.calc_ctr(bmark_mtd_ctr.fillna(0).loc[start_date:end_date]).reindex(
            self._strategy_order)

        result = gcm_ctr_stat.merge(bmark_ctr_stat, left_index=True, right_index=True)
        result[result.columns] = np.where(result[result.columns] == 0, None, result[result.columns])
        return result

    def get_standalone_rtn_rpt(self, gcm, bmark, start_date, end_date, trailing_period):
        gcm_cap_ror = self.get_returns_from_allocs(df=gcm, group_cols=['Date', 'Strategy'],
                                                   multiply_x='Ror',
                                                   multiply_y='pct_investment_of_portfolio_strategy_total')

        gcm_cap_stat = self.ann_return(
            gcm_cap_ror.pivot_table(index='Date', columns='Strategy', values='Ror').fillna(0).loc[start_date:end_date],
            trailing_periods=[trailing_period], freq=12) \
            .set_index('Strategy').reindex(self._strategy_order)

        gcm_eq_ror = self.get_returns_from_allocs(df=gcm, group_cols=['Date', 'Strategy'],
                                                  equal_weight=True)
        gcm_eql_stat = self.ann_return(
            gcm_eq_ror.pivot_table(index='Date', columns='Strategy', values='Ror').fillna(0).loc[start_date:end_date],
            trailing_periods=[trailing_period], freq=12) \
            .set_index('Strategy').reindex(self._strategy_order)

        bmark_eq_ror = self.get_returns_from_allocs(df=bmark, group_cols=['Date', 'Strategy'],
                                                    equal_weight=True)
        bmark_eql_stat = self.ann_return(
            bmark_eq_ror.pivot_table(index='Date', columns='Strategy', values='Ror').fillna(0).loc[start_date:end_date],
            trailing_periods=[trailing_period],
            freq=12) \
            .set_index('Strategy').reindex(self._strategy_order)

        result = gcm_cap_stat.merge(gcm_eql_stat, left_index=True, right_index=True). \
            merge(bmark_eql_stat, left_index=True, right_index=True)
        result[result.columns] = np.where(result[result.columns] == 0, None, result[result.columns])
        return result

    def get_allocation_rpt(self, gcm, bmark, start_date, end_date):
        gcm_alloc_start = gcm[gcm.Date == start_date][['Strategy', 'pct_strategy_of_portfolio_total']].rename(
            columns={'pct_strategy_of_portfolio_total': 'GcmAllocationStart'}) \
            .drop_duplicates().set_index('Strategy').reindex(self._strategy_order)
        bmark_alloc_start = bmark[bmark.Date == start_date][['Strategy', 'pct_strategy_of_total']].rename(
            columns={'pct_strategy_of_total': 'EhAllocationStart'}) \
            .drop_duplicates().set_index('Strategy').reindex(self._strategy_order)
        gcm_alloc_end = gcm[gcm.Date == end_date][['Strategy', 'pct_strategy_of_portfolio_total']].rename(
            columns={'pct_strategy_of_portfolio_total': 'GcmAllocationEnd'}) \
            .drop_duplicates().set_index('Strategy').reindex(self._strategy_order)
        bmark_alloc_end = bmark[bmark.Date == dt.date(2022, 7, 1)][['Strategy', 'pct_strategy_of_total']].rename(
            columns={'pct_strategy_of_total': 'EhAllocationEnd'}) \
            .drop_duplicates().set_index('Strategy').reindex(self._strategy_order)
        result = gcm_alloc_start.merge(bmark_alloc_start, left_index=True, right_index=True). \
            merge(gcm_alloc_end, left_index=True, right_index=True). \
            merge(bmark_alloc_end, left_index=True, right_index=True)
        return result

    def create_ts(self, df, idx, col, val):
        df_group = df.groupby([idx, col]).sum()[val].reset_index()
        pivot = df_group.pivot(index=idx, columns=col, values=val).resample('MS').sum()
        total_alloc = pivot.sum(axis=1)

        return pivot.divide(total_alloc, axis=0)

    def ann_return(self, returns, trailing_periods, freq=250, return_NoObs=False):
        result = pd.DataFrame()
        returns = returns.dropna()
        for trailing_period in trailing_periods:
            if trailing_period != "Incep":
                if len(returns) < trailing_period:
                    continue
                else:
                    return_sub = returns.tail(trailing_period)
            else:
                return_sub = returns
            if len(return_sub) <= 12:
                ann_return = pd.DataFrame(
                    pd.DataFrame((1 + return_sub).prod() - 1)
                )
            else:
                ann_return = pd.DataFrame(
                    return_sub.add(1).prod() ** (freq / len(return_sub)) - 1
                )
            ann_return["NoObs"] = len(return_sub)
            result = result.append(ann_return)
        result = result.reset_index().rename(
            columns={"index": "Name", 0: "AnnRor"}
        )
        if return_NoObs:
            return result
        else:
            return result.drop(columns=['NoObs'])

    def get_returns_from_allocs(self, df=None, group_cols=None, multiply_x=None, multiply_y=None, equal_weight=False):
        # example
        # group_cols = ['Acronym', 'Strategy']
        # multiply_x = 'Ror'
        # multiply_y = 'pct_investment_of_portfolio_strategy_total'

        if equal_weight:
            result = df.groupby(group_cols).Ror.mean().reset_index()
            return result

        else:
            df['Ctr'] = df[multiply_x] * df[multiply_y]
            result = df.groupby(group_cols).Ctr.sum().reset_index().rename(columns={'Ctr': 'Ror'})
            return result

    def get_allocation_data_portfolio(self, gcm=None, bmark=None, end_date=None):
        if gcm is None:
            gcm = self.get_ars_constituent_data_portfolio()
        if bmark is None:
            bmark = self.get_eh_constituent_data()

        gcm_total_aum = gcm.groupby('Date').OpeningBalance.sum().reset_index().rename(
            columns={'OpeningBalance': 'TotalDollar'})
        gcm_df = gcm.merge(gcm_total_aum, how='left', left_on='Date', right_on='Date')

        gcm_portfolio_total_aum = gcm_df.groupby(['Date', 'Acronym']).OpeningBalance.sum().reset_index().rename(
            columns={'OpeningBalance': 'TotalDollarPortfolio'})
        gcm_df = gcm_df.merge(gcm_portfolio_total_aum, how='left', left_on=['Date', 'Acronym'],
                              right_on=['Date', 'Acronym'])

        gcm_strategy_aum = gcm_df.groupby(['Date', 'Strategy']).OpeningBalance.sum().reset_index().rename(
            columns={'OpeningBalance': 'TotalDollarStrategy'})
        gcm_df = gcm_df.merge(gcm_strategy_aum, how='left', left_on=['Date', 'Strategy'],
                              right_on=['Date', 'Strategy'])

        gcm_portfolio_strategy_aum = gcm_df.groupby(
            ['Date', 'Acronym', 'Strategy']).OpeningBalance.sum().reset_index().rename(
            columns={'OpeningBalance': 'TotalDollarPortfolioStrategy'})
        gcm_df = gcm_df.merge(gcm_portfolio_strategy_aum, how='left', left_on=['Date', 'Acronym', 'Strategy'],
                              right_on=['Date', 'Acronym', 'Strategy'])

        # 1st degree
        gcm_df['pct_investment_of_total'] = gcm_df.OpeningBalance / gcm_df.TotalDollar
        gcm_df['pct_investment_of_portfolio_total'] = gcm_df.OpeningBalance / gcm_df.TotalDollarPortfolio
        gcm_df['pct_investment_of_strategy_total'] = gcm_df.OpeningBalance / gcm_df.TotalDollarStrategy
        gcm_df[
            'pct_investment_of_portfolio_strategy_total'] = gcm_df.OpeningBalance / gcm_df.TotalDollarPortfolioStrategy

        # second degree
        gcm_df['pct_strategy_of_total'] = gcm_df.TotalDollarStrategy / gcm_df.TotalDollar
        gcm_df['pct_strategy_of_portfolio_total'] = gcm_df.TotalDollarPortfolioStrategy / gcm_df.TotalDollarPortfolio

        # eh
        bmark_total_aum = bmark.groupby('Date').OpeningBalance.sum().reset_index().rename(
            columns={'OpeningBalance': 'TotalDollar'})
        bmark_df = bmark.merge(bmark_total_aum, how='left', left_on='Date', right_on='Date')

        bmark_strategy_aum = bmark_df.groupby(['Date', 'Strategy']).OpeningBalance.sum().reset_index().rename(
            columns={'OpeningBalance': 'TotalDollarStrategy'})
        bmark_df = bmark_df.merge(bmark_strategy_aum, how='left', left_on=['Date', 'Strategy'],
                                  right_on=['Date', 'Strategy'])

        bmark_df['pct_investment_of_total'] = bmark_df.OpeningBalance / bmark_df.TotalDollar
        bmark_df['pct_investment_of_strategy_total'] = bmark_df.OpeningBalance / bmark_df.TotalDollarStrategy

        # second degree
        bmark_df['pct_strategy_of_total'] = bmark_df.TotalDollarStrategy / bmark_df.TotalDollar

        # Betas
        # if start_date is None:
        #     start_date = dt.date(1900, 1, 1)
        # if end_date is None:
        #     end_date = dt.date(2099, 12, 31)
        # y3 = start_date - relativedelta(months=12 * 3 - 1)
        # gcm_ret = self.get_gcm_returns(y3, end_date)
        # gcm_betas = self.calc_beta(gcm_ret, y3, end_date)
        #
        # ehi_ret = self.get_eh_returns(y3, end_date)
        # ehi_betas = self.calc_beta(ehi_ret, y3, end_date)
        #
        # gcm_df['Beta'] = gcm_df['InvestmentGroupName'].map(gcm_betas.to_dict())
        # eh_df['Beta'] = eh_df['InvestmentGroupname'].map(ehi_betas.to_dict())
        #
        # gcm_df = self.calc_beta_bucket(gcm_df)
        # eh_df = self.calc_beta_bucket(eh_df)
        #
        # gcm_beta_aum = gcm_df.groupby(['Date', 'Beta Bucket']).OpeningBalance.sum().reset_index().rename(
        #     columns={'OpeningBalance': 'TotalDollarBetaBucket'})
        # gcm_df = gcm_df.merge(gcm_beta_aum, how='left', left_on=['Date', 'Beta Bucket'],
        #                       right_on=['Date', 'Beta Bucket'])
        #
        # gcm_portfolio_beta_aum = gcm_df.groupby(['Date', 'Acronym', 'Beta Bucket']).OpeningBalance.sum().reset_index().rename(
        #     columns={'OpeningBalance': 'TotalDollarPortfolioBetaBucket'})
        # gcm_df = gcm_df.merge(gcm_portfolio_beta_aum, how='left', left_on=['Date', 'Acronym', 'Beta Bucket'],
        #                       right_on=['Date', 'Acronym', 'Beta Bucket'])
        # gcm_df['pct_investment_of_beta_total'] = gcm_df.OpeningBalance / gcm_df.TotalDollarBetaBucket
        # gcm_df['pct_investment_of_portfolio_beta_total'] = gcm_df.OpeningBalance / gcm_df.TotalDollarPortfolioBetaBucket
        #
        # gcm_df['pct_beta_of_total'] = gcm_df.TotalDollarBetaBucket/ gcm_df.TotalDollar
        # gcm_df['pct_beta_of_portfolio_total'] = gcm_df.TotalDollarPortfolioBetaBucket / gcm_df.TotalDollarPortfolio
        #
        # eh_beta_aum = eh_df.groupby(['Date', 'Beta Bucket']).OpeningBalance.sum().reset_index().rename(
        #     columns={'OpeningBalance': 'TotalDollarBetaBucket'})
        # eh_df = eh_df.merge(eh_beta_aum, how='left', left_on=['Date', 'Beta Bucket'],
        #                     right_on=['Date', 'Beta Bucket'])
        # eh_df['pct_investment_of_beta_total'] = eh_df.OpeningBalance / eh_df.TotalDollarBetaBucket
        # eh_df['pct_beta_of_total'] = eh_df.TotalDollarBetaBucket / eh_df.TotalDollar

        return gcm_df, bmark_df

    def get_allocation_data_firmwide(self, gcm=None, bmark=None, end_date=None):
        if gcm is None:
            gcm = self.get_ars_constituent_data_firmwide()
        if bmark is None:
            bmark = self.get_eh_constituent_data()

        gcm = gcm[gcm.OpeningBalance >= 5e6]
        gcm_total_aum = gcm.groupby('Date').OpeningBalance.sum().reset_index().rename(
            columns={'OpeningBalance': 'TotalDollar'})
        gcm_df = gcm.merge(gcm_total_aum, how='left', left_on='Date', right_on='Date')

        gcm_strategy_aum = gcm_df.groupby(['Date', 'Strategy']).OpeningBalance.sum().reset_index().rename(
            columns={'OpeningBalance': 'TotalDollarStrategy'})
        gcm_df = gcm_df.merge(gcm_strategy_aum, how='left', left_on=['Date', 'Strategy'],
                              right_on=['Date', 'Strategy'])

        # 1st degree
        gcm_df['pct_investment_of_total'] = gcm_df.OpeningBalance / gcm_df.TotalDollar
        gcm_df['pct_investment_of_strategy_total'] = gcm_df.OpeningBalance / gcm_df.TotalDollarStrategy

        # second degree
        gcm_df['pct_strategy_of_total'] = gcm_df.TotalDollarStrategy / gcm_df.TotalDollar

        # bmark
        bmark_total_aum = bmark.groupby('Date').OpeningBalance.sum().reset_index().rename(
            columns={'OpeningBalance': 'TotalDollar'})
        bmark_df = bmark.merge(bmark_total_aum, how='left', left_on='Date', right_on='Date')

        bmark_strategy_aum = bmark_df.groupby(['Date', 'Strategy']).OpeningBalance.sum().reset_index().rename(
            columns={'OpeningBalance': 'TotalDollarStrategy'})
        bmark_df = bmark_df.merge(bmark_strategy_aum, how='left', left_on=['Date', 'Strategy'],
                                  right_on=['Date', 'Strategy'])

        bmark_df['pct_investment_of_total'] = bmark_df.OpeningBalance / bmark_df.TotalDollar
        bmark_df['pct_investment_of_strategy_total'] = bmark_df.OpeningBalance / bmark_df.TotalDollarStrategy

        # second degree
        bmark_df['pct_strategy_of_total'] = bmark_df.TotalDollarStrategy / bmark_df.TotalDollar

        return gcm_df, bmark_df

    def get_eh_constituent_data(self):
        eh_allocs = self.get_eh_with_inv_group()
        default_peer = self._strategy_benchmark.get_default_peer_benchmarks(investment_group_names=None)[['InvestmentGroupName', 'ReportingPeerGroup']]
        substrat_map = pd.read_csv('C:\Code\Gcm.RiskScripts\Analyses\GCM_ARS_vs_Market_Analysis\SubStrategyPeerMap.csv')
        substrat_map.ReportingPeerGroupOverride = np.where(substrat_map.InvestmentSubStrategy == 'Event Driven',
                                                           'GCM Multi-Strategy',
                                                           substrat_map.ReportingPeerGroupOverride)
        peer_to_strat_map = pd.read_csv(
            'C:\Code\Gcm.RiskScripts\Analyses\GCM_ARS_vs_Market_Analysis\PeerToStrategyMap.csv')

        eh_default_peer = eh_allocs.merge(default_peer, how='left', left_on='InvestmentGroupName',
                                          right_on='InvestmentGroupName')

        gcm_map = self._entity_master.get_investment_group_strategies()
        eh_default_peer = eh_default_peer.merge(gcm_map, how='left', left_on='InvestmentGroupName',
                                                right_on='InvestmentGroupName')

        eh_default_peer = eh_default_peer.merge(substrat_map, how='left', left_on='GcmSubStrategy',
                                                right_on='InvestmentSubStrategy')
        eh_default_peer = eh_default_peer.merge(
            substrat_map.rename(columns={'InvestmentSubStrategy': 'EurekaStrategyDrop',
                                         'ReportingPeerGroupOverride': 'EurekaGroupOverride'}), how='left',
            left_on='EurekaStrategy',
            right_on='EurekaStrategyDrop')

        eh_default_peer.ReportingPeerGroup = np.where(eh_default_peer.ReportingPeerGroup.isnull(),
                                                      eh_default_peer.ReportingPeerGroupOverride,
                                                      eh_default_peer.ReportingPeerGroup)
        eh_default_peer.ReportingPeerGroup = np.where(eh_default_peer.ReportingPeerGroup.isnull(),
                                                      eh_default_peer.EurekaGroupOverride,
                                                      eh_default_peer.ReportingPeerGroup)

        eh_result = eh_default_peer.merge(peer_to_strat_map, how='left', left_on='ReportingPeerGroup',
                                          right_on='ReportingPeerGroup')
        eh_result = eh_result.merge(peer_to_strat_map.rename(columns={'Strategy': 'TopDogOverride'}), how='left',
                                    left_on='EurekaGroupOverride',
                                    right_on='ReportingPeerGroup')

        eh_result.Strategy = np.where(eh_result.Strategy.isnull(), eh_result.TopDogOverride, eh_result.Strategy)

        name_overrides = pd.read_csv(
            'C:\Code\Gcm.RiskScripts\Analyses\GCM_ARS_vs_Market_Analysis\EurekahedgeStrategyOverrides.csv')
        eh_result = eh_result.merge(name_overrides, how='left', left_on='InvestmentName', right_on='Fund')

        eh_result.Strategy = np.where(~eh_result.StrategyOverride.isnull(), eh_result.StrategyOverride,
                                      eh_result.Strategy)

        eh_result['OpeningBalance'] = 100
        result = eh_result[['Date', 'Strategy', 'InvestmentName', 'OpeningBalance', 'Ror']].rename(
            columns={'InvestmentName': 'InvestmentGroupName'}).drop_duplicates()

        result.Date = pd.to_datetime(result.Date).dt.date
        assert (len(result) == len(eh_allocs))
        assert (len(result[result.Strategy.isnull()]) == 0)
        return result

    def get_eh_in_gcm_strat(self):
        def my_dao_operation(dao, params):
            raw = "select distinct v.InvestmentGroupName, first_value(id.Strategy) OVER (PARTITION BY v.InvestmentGroupName ORDER BY id.Strategy ASC ROWS UNBOUNDED PRECEDING) GcmStrategy, first_value(id.SubStrategy) OVER (PARTITION BY v.InvestmentGroupName ORDER BY id.Strategy ASC ROWS UNBOUNDED PRECEDING) GcmSubStrategy from entitymaster.vInvestments v left join altsoft.InvestmentDimn id on v.SourceInvestmentId = id.SourceInvestmentId where v.SourceName = 'AltSoft.Pub' and id.SourceName = 'AltSoft.Pub' and id.Strategy is not null"
            df = pd.read_sql(
                raw,
                dao.data_engine.session.bind,
            )
            return df

        df = self._runner.execute(
            params={},
            source=DaoSource.InvestmentsDwh,
            operation=my_dao_operation,
        )
        return df

    def get_eh_with_inv_group(self):
        eh = self._strategy_benchmark.get_eurekahedge_constituents(benchmarks_names=['Eurekahedge Institutional 200'], start_date=dt.date(2010, 1, 1), end_date=self._report_date)
        # eh_groups = self.get_eh_inv_groups()
        eh_groups = self._entity_master.get_investment_entities(source_names=['AltSoft.Eurekahedge'])[['SourceInvestmentId', 'InvestmentGroupName', ]].drop_duplicates()

        eh.SourceInvestmentId = 'EH' + eh.SourceInvestmentId.astype(str)

        eh_joined = eh.merge(eh_groups, how='left', left_on='SourceInvestmentId', right_on='SourceInvestmentId')
        eh_joined.InvestmentGroupName = np.where(eh_joined.InvestmentGroupName.isnull(), eh_joined.InvestmentName,
                                                 eh_joined.InvestmentGroupName)

        assert (len(eh_joined) == len(eh))
        assert (len(eh_joined[eh_joined.InvestmentGroupName.isnull()]) == 0)
        return eh_joined

    # TODO: refactor to use get_investments
    def get_eh_inv_groups(self):
        def my_dao_operation(dao, params):
            raw = "select distinct SourceInvestmentId, InvestmentGroupName from entitymaster.vInvestments where SourceName = 'AltSoft.Eurekahedge'"
            df = pd.read_sql(
                raw,
                dao.data_engine.session.bind,
            )
            return df

        df = self._runner.execute(
            params={},
            source=DaoSource.InvestmentsDwh,
            operation=my_dao_operation,
        )
        return df

    def get_ars_constituent_data_firmwide(self):
        gcm_balances = self.get_ars_firmwide_balances_with_peer_group()
        # gcm_ror = self.get_ars_portfolio_investment_returns()

        # gcm_joined = gcm_balances.merge(gcm_ror, how='left', left_on=['Date', 'Acronym', 'InvestmentGroupName'], right_on=['Date', 'Acronym', 'InvestmentGroupName'])

        # where multiple ROR for investment group/date/acronym; takes ROR from largest balance
        result = gcm_balances.sort_values('OpeningBalance', ascending=False).drop_duplicates(
            ['Date', 'InvestmentGroupName', 'OpeningBalance', 'Strategy', 'Pnl'])
        result['Ror'] = gcm_balances.Pnl / gcm_balances.OpeningBalance
        result.Ror = result.Ror.fillna(0)
        result.OpeningBalance = result.OpeningBalance.fillna(0)

        result.Date = pd.to_datetime(result.Date).dt.date
        result = result[~result.Strategy.isnull()]
        # ensure no dups, ensure all is mapped
        # assert (len(result) == len(gcm_balances))
        # assert (len(result[result.Strategy.isnull()]) == 0)
        return result

    def get_ars_constituent_data_portfolio(self):
        gcm_balances = self.get_ars_portfolio_balances_with_peer_group()
        # gcm_ror = self.get_ars_portfolio_investment_returns()

        # gcm_joined = gcm_balances.merge(gcm_ror, how='left', left_on=['Date', 'Acronym', 'InvestmentGroupName'], right_on=['Date', 'Acronym', 'InvestmentGroupName'])

        # where multiple ROR for investment group/date/acronym; takes ROR from largest balance
        result = gcm_balances.sort_values('OpeningBalance', ascending=False).drop_duplicates(
            ['Date', 'Acronym', 'InvestmentGroupName', 'OpeningBalance', 'Strategy', 'Pnl'])
        result['Ror'] = gcm_balances.Pnl / gcm_balances.OpeningBalance
        result.Ror = result.Ror.fillna(0)
        result.OpeningBalance = result.OpeningBalance.fillna(0)

        result.Date = pd.to_datetime(result.Date).dt.date

        # removing ['Northwood Liquid Opps Ltd', 'Waterfront CP Ltd'] currently 9/25
        result = result[~result.Strategy.isnull()]

        # ensure no dups, ensure all is mapped
        # assert (len(result) == len(gcm_balances))
        # assert (len(result[result.Strategy.isnull()]) == 0)
        return result

    def get_ars_portfolio_investment_returns(self):
        def my_dao_operation(dao, params):
            raw = "SELECT FORMAT(PeriodDate,'yyyy-MM-01') Date, Acronym, ISNULL(InvestmentGroupName, InvestmentName) InvestmentGroupName, RateOfReturn Ror FROM [analytics].[PortfolioInvestmentReturns] where IsActive = 'True' and IsGcmPortfolio = 'True' and RateOfReturn is not NULL and RateOfReturn != 0 and EOMONTH(PeriodDate) = PeriodDate"
            df = pd.read_sql(
                raw,
                dao.data_engine.session.bind,
            )
            return df

        df = self._runner.execute(
            params={},
            source=DaoSource.PubDwh,
            operation=my_dao_operation,
        )
        result = df[['Date', 'Acronym', 'InvestmentGroupName', 'Ror']]
        return result

    def map_gcm_strategies_to_peer_group(self, gcm_allocs):
        default_peer = self._strategy_benchmark.get_default_peer_benchmarks(investment_group_names=None)[['InvestmentGroupName', 'ReportingPeerGroup']]
        # substrat_map = pd.read_csv('Reports\\reports\\brinson_based_attribution\\input_data\\SubStrategyPeerMap.csv')
        substrat_map = pd.read_csv('C:\Code\Gcm.RiskScripts\Analyses\GCM_ARS_vs_Market_Analysis\SubStrategyPeerMap.csv')

        gcm_default_peer = gcm_allocs.merge(default_peer, how='left', left_on='InvestmentGroupName',
                                            right_on='InvestmentGroupName'). \
            merge(substrat_map, how='left', left_on='SubStrategy', right_on='InvestmentSubStrategy')
        gcm_default_peer['ReportingPeerGroup'] = np.where(gcm_default_peer.ReportingPeerGroup.isnull(),
                                                          gcm_default_peer.ReportingPeerGroupOverride,
                                                          gcm_default_peer.ReportingPeerGroup)
        gcm_default_peer = gcm_default_peer[gcm_default_peer.ReportingPeerGroup != 'Other']
        gcm_default_peer['ReportingPeerGroup'] = np.where((gcm_default_peer.ReportingPeerGroup == 'GCM Multi-PM') & (
                gcm_default_peer.ReportingPeerGroupOverride == 'GCM Equities'), 'GCM Equities',
                                                          gcm_default_peer.ReportingPeerGroup)
        gcm_default_peer['ReportingPeerGroup'] = np.where(
            (gcm_default_peer.ReportingPeerGroup == 'GCM Diversifying Strategies') & (
                    gcm_default_peer.ReportingPeerGroupOverride == 'GCM Equities'), 'GCM Equities',
            gcm_default_peer.ReportingPeerGroup)

        gcm_default_peer['ReportingPeerGroup'] = np.where(gcm_default_peer.ReportingPeerGroup == 'GCM Asia',
                                                          gcm_default_peer.ReportingPeerGroupOverride,
                                                          gcm_default_peer.ReportingPeerGroup)
        # move to csv: overriding some standout managers
        gcm_default_peer['ReportingPeerGroup'] = np.where(gcm_default_peer.InvestmentGroupName == 'Elliott',
                                                          gcm_default_peer.ReportingPeerGroupOverride,
                                                          gcm_default_peer.ReportingPeerGroup)
        gcm_default_peer['ReportingPeerGroup'] = np.where(gcm_default_peer.InvestmentGroupName == 'Atlas Enhanced Fund',
                                                          'GCM Equities',
                                                          gcm_default_peer.ReportingPeerGroup)
        gcm_default_peer['ReportingPeerGroup'] = np.where(
            gcm_default_peer.InvestmentGroupName == 'Brevan Howard FG Macro',
            'GCM Macro',
            gcm_default_peer.ReportingPeerGroup)
        gcm_default_peer['ReportingPeerGroup'] = np.where(
            gcm_default_peer.InvestmentGroupName == 'Alphadyne Global Rates II',
            'GCM Macro',
            gcm_default_peer.ReportingPeerGroup)
        gcm_default_peer['ReportingPeerGroup'] = np.where(
            gcm_default_peer.InvestmentGroupName == 'Alphadyne Intl Master',
            'GCM Macro',
            gcm_default_peer.ReportingPeerGroup)

        gcm_default_peer['ReportingPeerGroup'] = np.where(
            gcm_default_peer.InvestmentGroupName == 'Magnetar Energy',
            'GCM Credit',
            gcm_default_peer.ReportingPeerGroup)

        gcm_default_peer['ReportingPeerGroup'] = np.where(
            gcm_default_peer.InvestmentGroupName == 'Magnetar Energy Opp',
            'GCM Credit',
            gcm_default_peer.ReportingPeerGroup)

        gcm_default_peer['ReportingPeerGroup'] = np.where(
            gcm_default_peer.InvestmentGroupName == 'Davidson Kempner',
            'GCM Multi-Strategy',
            gcm_default_peer.ReportingPeerGroup)

        peer_to_strat_map = pd.read_csv(
            'C:\Code\Gcm.RiskScripts\Analyses\GCM_ARS_vs_Market_Analysis\PeerToStrategyMap.csv')

        gcm_strategy_balance = gcm_default_peer.merge(peer_to_strat_map, how='left', left_on='ReportingPeerGroup',
                                                      right_on='ReportingPeerGroup')
        return gcm_strategy_balance

    def get_ars_firmwide_balances_with_peer_group(self):
        gcm_allocs = self._investment_group.get_firmwide_allocation(start_date=dt.date(2010, 1, 1), end_date=self._report_date)
        gcm_strategy_balance = self.map_gcm_strategies_to_peer_group(gcm_allocs.drop(columns=['Strategy']))

        ###
        result = gcm_strategy_balance[
            ['Date', 'InvestmentGroupName', 'OpeningBalance', 'Strategy', 'Pnl']].drop_duplicates()

        result = result[(~result.OpeningBalance.isnull()) & (result.OpeningBalance != 0)]
        return result

    def get_ars_portfolio_balances_with_peer_group(self):
        gcm_allocs = self._portfolio.get_holdings(start_date=dt.date(2010, 1, 1), end_date=self._report_date, lookthrough='True').drop(columns=['Strategy'])
        gcm_strategy_balance = self.map_gcm_strategies_to_peer_group(gcm_allocs)

        ###
        result = gcm_strategy_balance[
            ['Date', 'Acronym', 'InvestmentGroupName', 'OpeningBalance', 'Strategy', 'Pnl']].drop_duplicates()

        result = result[(~result.OpeningBalance.isnull()) & (result.OpeningBalance != 0)]
        return result


    def get_acronyms_for_bba(self, report_date):
        def my_dao_operation(dao, params):
            # raw = "SELECT FORMAT(PeriodDate,'yyyy-MM-01') Date, Acronym, ISNULL(InvestmentGroupName, InvestmentName) InvestmentGroupName, InvestmentSubStrategy, sum(OpeningBalanceUsd) OpeningBalance, sum(EndingBalanceUsd) EndingBalance, sum(EndingBalanceUsd) + sum(EndingNetActivityUsd) as EndingBalanceWithActivity FROM [analytics].[InvestmentFinancialBalances] where IsMonthEnd = 1 and IsActive = 'True' and IsGcmPortfolio = 'True' group by Acronym, InvestmentSubStrategy, ISNULL(InvestmentGroupName, InvestmentName), FORMAT(PeriodDate,'yyyy-MM-01')"
            raw = "SELECT FORMAT(PeriodDate,'yyyy-MM-01') Date, Acronym, ISNULL(InvestmentGroupName, InvestmentName) InvestmentGroupName, InvestmentSubStrategy, sum(OpeningBalanceUsd) OpeningBalance, sum(EndingBalanceUsd) EndingBalance, sum(InvestmentResultUSD) Pnl FROM [analytics].[PortfolioHoldings] where IsMonthEnd = 'True' and IsActive = 'True' and IsGcmPortfolio = 'True' and IsLookThru='True' and ScenarioType='Actual' group by Acronym, InvestmentSubStrategy, ISNULL(InvestmentGroupName, InvestmentName), FORMAT(PeriodDate,'yyyy-MM-01')"

            df = pd.read_sql(
                raw,
                dao.data_engine.session.bind,
            )
            return df

        df = self._runner.execute(
            params={},
            source=DaoSource.PubDwh,
            operation=my_dao_operation,
        )
        result = df[['Date', 'Acronym', 'InvestmentGroupName', 'InvestmentSubStrategy', 'OpeningBalance', 'Pnl'
                     ]]
        return result

    def get_gcm_returns(self, start_date, end_date):
        ret = dao.get_all_EMM_mgr_return_with_strategy(self._runner, start_date, end_date)
        ret['ID'] = ret['InvestmentGroupName'].fillna(ret['InvestmentName'])

        # Senator Global Opp has duplicate entries due to multiple strategies mapped to same mgr - REMOVE Duplicates manually
        drop_idx = ret.loc[(ret['ID'] == 'Senator Global Opp') & (ret['InvestmentStrategy'] == 'Multi-Strategy')].index
        ret.drop(index=drop_idx, inplace=True)

        # Drop duplicates
        ret = ret.drop_duplicates(subset=['ID', 'PeriodDate'])

        ret = ret.pivot(index='PeriodDate', columns='ID', values='RateOfReturn')

        return ret

    def get_eh_returns(self, start_date, end_date):
        ret = dao.get_EH_ret_by_mgr(self._runner, start_date, end_date)
        ret = ret.pivot(index='Date', columns='InvestmentName', values='Ror')

        return ret

    def calc_beta(self, df, start_date, end_date):

        sp500 = dao.get_index_returns(["S&P 500"], self._runner, start_date, end_date)
        betas = df.loc[start_date:end_date].apply(
            lambda x: ql.beta(x, sp500.loc[start_date:end_date, 'RateOfReturn'], 12), axis=0)

        return betas

    def calc_beta_bucket(self, df):
        df['Beta Bucket'] = 'Insufficient Data'
        df.loc[round(df['Beta'], 2) < 0.1, 'Beta Bucket'] = '< 0.1'
        df.loc[(round(df['Beta'], 2) >= 0.1) & (round(df['Beta'], 2) <= 0.25), 'Beta Bucket'] = '0.1 - 0.25'
        df.loc[(round(df['Beta'], 2) > 0.25) & (round(df['Beta'], 2) <= 0.5), 'Beta Bucket'] = '0.26 - 0.5'
        df.loc[(round(df['Beta'], 2) > 0.5) & (round(df['Beta'], 2) <= 1.0), 'Beta Bucket'] = '0.5 - 1.0'
        df.loc[round(df['Beta'], 2) > 1.0, 'Beta Bucket'] = '> 1'

        return df

    def calc_ctr(self, df):
        result = pd.DataFrame(columns=['CTR'], index=df.columns)
        rors = df.sum(axis=1)

        for col in df.columns:
            ctr = df.loc[:, col].tolist()
            ror_to_date = (1 + rors).cumprod() - 1
            idx = 1
            res = ctr[0]
            for c in ctr[1:]:
                res += c * (1 + ror_to_date[idx - 1])
                idx += 1
            result.loc[col, 'CTR'] = res

        return result

    # def calc_ctr(self,df):
    #     '''
    #     :param ror: Dataframe with index = Date, columns = Cap-Weighted RoR Series (column name agnostic)
    #     :param cap_df: Dataframe with index = Date, columns = Monthly CTR (column name agnostic)
    #     :return: total CTR over full time period passed
    #     '''
    #
    #     # Create res to capture results
    #     final = pd.DataFrame(columns = ['CTR'], index = df.columns)
    #
    #     # Get Dates to Run Icelandic Approach on
    #     dates = df.index.unique()
    #
    #     # Calculate each month's contribution
    #     for col in df.columns:
    #         ctr = []
    #         ror_to_date = []
    #         for d in dates:
    #             ctr_temp = df.loc[d, col]
    #             ctr.append(ctr_temp)
    #             rors = df.loc[d].sum()
    #             ror_to_date.append(ql.cumulative_rets(rors,12))
    #
    #         idx = 1
    #         res = ctr[0]
    #         for c in ctr[1:]:
    #             res += c * (1 + ror_to_date[idx - 1])
    #             idx += 1
    #         final.loc[col, 'CTR'] = res
    #
    #     return final

    def avg_strat_differences(self, gcm, eh):
        res = pd.DataFrame(
            columns=['Credit', 'Long/Short Equity', 'Macro', 'Multi-Strategy', 'Quantitative', 'Relative Value'],
            index=gcm['Acronym'].unique())

        # Calculate EHI Strategy Allocations each Month
        eh_ts = eh[['Date', 'Strategy', 'pct_strategy_of_total']].drop_duplicates().pivot(index='Date',
                                                                                          columns='Strategy',
                                                                                          values='pct_strategy_of_total').fillna(
            0)

        for port in gcm['Acronym'].unique():
            gcm_ts = gcm.loc[gcm['Acronym'] == port][
                ['Date', 'Strategy', 'pct_strategy_of_portfolio_total']].drop_duplicates().pivot(index='Date',
                                                                                                 columns='Strategy',
                                                                                                 values='pct_strategy_of_portfolio_total').fillna(
                0)
            diff = (gcm_ts - eh_ts).fillna(-eh_ts)
            diff_avg = diff.mean()
            res.loc[port] = diff_avg

        return res

    def return_diff(self, gcm, eh_index_name, start_date, end_date):
        port_list = gcm.loc[(gcm['Date'] >= start_date) & (gcm['Date'] <= end_date), 'Acronym'].unique()
        res = pd.DataFrame(columns=['GCM', 'EH', 'Delta'], index=port_list)

        port_ret = dao.get_portfolio_ret(res.index.to_list(), self._runner, start_date, end_date)
        eh_ret = dao.get_EH_ret(eh_index_name, self._runner)
        ret_eh = ql.cumulative_rets(eh_ret.loc[port_ret.index[0]:port_ret.index[-1]]['Ror'], 12)

        for port in res.index:
            if port in port_ret.columns:
                ret = ql.cumulative_rets(port_ret[port], 12)
                res.loc[port] = [ret, ret_eh, (ret - ret_eh)]
            else:
                print(f"{port} NO RETURNS - SKIPPED")
                continue

        return res

    def generate_firmwide_report(self, acronyms):
        ws = 'Summary'
        self._wb = load_workbook(
            r'C:\Code\Gcm.RiskScripts\Analyses\GCM_ARS_vs_Market_Analysis\BBA_Template_Firm.xlsx')
        self._wb['Summary'].cell(row=4, column=15,
                                 value=(self._report_date.replace(day=1) + dt.timedelta(days=31)).replace(
                                     day=1) - dt.timedelta(
                                     days=1))

        df = self._gcm_firmwide
        df.rename(columns={'pct_investment_of_total': 'pct_investment_of_portfolio_total',
                                                'pct_investment_of_strategy_total': 'pct_investment_of_portfolio_strategy_total',
                                                'pct_strategy_of_total': 'pct_strategy_of_portfolio_total'}, inplace=True)


        #################################### ytd section #########################################
        ytd_start_date = dt.date(self._report_date.year, 1, 1)
        # allocations

        ytd_allocs = self.get_allocation_rpt(gcm=df, bmark=self._eh, start_date=ytd_start_date,
                                                     end_date=self._report_date)
        # get_standalone_return
        ytd_standalone_rtn = self.get_standalone_rtn_rpt(gcm=df, bmark=self._eh, start_date=ytd_start_date,
                                                                 end_date=self._report_date,
                                                                 trailing_period=self._report_date.month)

        # get_ctr
        ytd_ctr = self.get_ctr_rpt(gcm=df, bmark=self._eh, start_date=ytd_start_date,
                                           end_date=self._report_date, trailing_period=self._report_date.month)
        # get_attribution
        ytd_attrib = self.get_attribution_rpt(gcm=df, bmark=self._eh, start_date=ytd_start_date,
                                                      end_date=self._report_date,
                                                      trailing_period=self._report_date.month)
        #################################### ttm section #########################################
        ttm_start_date = self._report_date - relativedelta(years=1) + relativedelta(months=1)
        # allocations
        ttm_allocs = self.get_allocation_rpt(gcm=df, bmark=self._eh, start_date=ttm_start_date,
                                                     end_date=self._report_date)
        # get_standalone_return
        ttm_standalone_rtn = self.get_standalone_rtn_rpt(gcm=df, bmark=self._eh, start_date=ttm_start_date,
                                                                 end_date=self._report_date,
                                                                 trailing_period=12)
        # get_ctr
        ttm_ctr = self.get_ctr_rpt(gcm=df, bmark=self._eh, start_date=ttm_start_date,
                                           end_date=self._report_date, trailing_period=12)
        # get_attribution
        ttm_attrib = self.get_attribution_rpt(gcm=df, bmark=self._eh, start_date=ttm_start_date,
                                                      end_date=self._report_date,
                                                      trailing_period=12)
        #################################### 3y section #########################################
        three_y_start_date = self._report_date - relativedelta(years=3) + relativedelta(months=1)
        # allocations
        three_y_allocs = self.get_allocation_rpt(gcm=df, bmark=self._eh, start_date=three_y_start_date,
                                                         end_date=self._report_date)
        # get_standalone_return
        three_y_standalone_rtn = self.get_standalone_rtn_rpt(gcm=df, bmark=self._eh,
                                                                     start_date=three_y_start_date,
                                                                     end_date=self._report_date,
                                                                     trailing_period=36)
         # get_ctr
        three_y_ctr = self.get_ctr_rpt(gcm=df, bmark=self._eh, start_date=three_y_start_date,
                                               end_date=self._report_date, trailing_period=36)
         # get_attribution
        three_y_attrib = self.get_attribution_rpt(gcm=df, bmark=self._eh, start_date=three_y_start_date,
                                                          end_date=self._report_date,
                                                          trailing_period=36)

        ########### outliers attribution #################

        top_10_strategy_sizing, bottom_10_strategy_sizing, \
        top_10_manager_sizing, bottom_10_manager_sizing, \
        top_10_manager_selection, bottom_10_manager_selection, \
        top_10_strategy_selection, bottom_10_strategy_selection = self.get_portfolio_bba_outliers(acronyms=acronyms, gcm=self._gcm, bmark=self._eh, start_date=ttm_start_date,
                                                                                                  end_date=self._report_date, trailing_period=self._report_date.month)
        # get excess table
        bmark_rtn = self.get_ehi_returns()
        excess_return_total = self.get_excess_return_rpt(port_rtn=df, bmark_rtn=bmark_rtn)

        input_data = {
            "ytd_allocs": ytd_allocs,
            "ytd_standalone_rtn": ytd_standalone_rtn,
            "ytd_ctr": ytd_ctr,
            "ytd_attrib": ytd_attrib,
            "ttm_allocs": ttm_allocs,
            "ttm_standalone_rtn": ttm_standalone_rtn,
            "ttm_ctr": ttm_ctr,
            "ttm_attrib": ttm_attrib,
            "three_y_allocs": three_y_allocs,
            "three_y_standalone_rtn": three_y_standalone_rtn,
            "three_y_ctr": three_y_ctr,
            "three_y_attrib": three_y_attrib,
            "top_10_strategy_sizing": top_10_strategy_sizing,
            "bottom_10_strategy_sizing": bottom_10_strategy_sizing,
            "top_10_manager_sizing": top_10_manager_sizing,
            "bottom_10_manager_sizing": bottom_10_manager_sizing,
            "top_10_manager_selection": top_10_manager_selection,
            "bottom_10_manager_selection": bottom_10_manager_selection,
            "top_10_strategy_selection": top_10_strategy_selection,
            "bottom_10_strategy_selection": bottom_10_strategy_selection,
            "excess_return_total": excess_return_total
        }

        # old - replace with template
        # self._wb.save('{}{:%Y%m%d}_{}_BBA.xlsx'.format(self._output_dir,
        #                                                (self._report_date.replace(day=1) + dt.timedelta(
        #                                                    days=31)).replace(
        #                                                    day=1) - dt.timedelta(days=1),
        #                                                'Firm'))



    def generate_portfolio_report(self, acronym):
        print(acronym)

        ws = 'Summary'
        self._wb = load_workbook(r'C:\Code\Gcm.RiskScripts\Analyses\GCM_ARS_vs_Market_Analysis\BBA_Template_Portfolio.xlsx')
        self._wb['Summary'].cell(row=4, column=15,
                                 value=(self._report_date.replace(day=1) + dt.timedelta(days=31)).replace(day=1) - dt.timedelta(
                                     days=1))
        self._wb['Summary'].cell(row=3, column=15,
                                 value=acronym)

        df = self._gcm[(self._gcm.Acronym == acronym)]
        port_rtn = self.get_portfolio_ret(acronym=[acronym])
        bmark_rtn = self.get_ehi_returns()

        # get_ror
        rtn_df = self.get_returns_rpt(port_rtn, bmark_rtn)
        #get_beta
        beta_df = self.get_betas_rpt(port_rtn=port_rtn, bmark_rtn=bmark_rtn, assets=[acronym, 'EHI200'], benchmarks=['SPXT'])
        #get_downside
        downside_df = self.get_downside_rpt(port_rtn=port_rtn, bmark_rtn=bmark_rtn, assets=[acronym, 'EHI200'], benchmarks=['SPXT'])
        #get_correlation
        correl_df = self.get_correlation_rpt(port_rtn=port_rtn, bmark_rtn=bmark_rtn, assets=[acronym, 'EHI200'],
                                                    benchmarks=['SPXT'])
        #get_vol
        vol_df = self.get_vol_rpt(port_rtn=port_rtn, bmark_rtn=bmark_rtn)
        #get_sharpe
        sharpe_df = self.get_sharpe_rpt(port_rtn=port_rtn, bmark_rtn=bmark_rtn)
        #################################### ytd section #########################################
        ytd_start_date = dt.date(self._report_date.year, 1, 1)
        # allocations
        ytd_allocs = self.get_allocation_rpt(gcm=df, bmark=self._eh, start_date=ytd_start_date, end_date=self._report_date)
        # get_standalone_return
        ytd_standalone_rtn = self.get_standalone_rtn_rpt(gcm=df, bmark=self._eh, start_date=ytd_start_date, end_date=self._report_date, trailing_period=self._report_date.month)
        # get_ctr
        ytd_ctr = self.get_ctr_rpt(gcm=df, bmark=self._eh, start_date=ytd_start_date, end_date=self._report_date, trailing_period=self._report_date.month)
        # get_attribution
        ytd_attrib = self.get_attribution_rpt(gcm=df, bmark=self._eh, start_date=ytd_start_date, end_date=self._report_date, trailing_period=self._report_date.month)
        #################################### ttm section #########################################
        ttm_start_date = self._report_date - relativedelta(years=1) + relativedelta(months=1)
        # allocations
        ttm_allocs = self.get_allocation_rpt(gcm=df, bmark=self._eh, start_date=ttm_start_date,
                                                     end_date=self._report_date)
        # get_standalone_return
        ttm_standalone_rtn = self.get_standalone_rtn_rpt(gcm=df, bmark=self._eh, start_date=ttm_start_date,
                                                                 end_date=self._report_date,
                                                                 trailing_period=12)
        # get_ctr
        ttm_ctr = self.get_ctr_rpt(gcm=df, bmark=self._eh, start_date=ttm_start_date,
                                           end_date=self._report_date, trailing_period=12)
        # get_attribution
        ttm_attrib = self.get_attribution_rpt(gcm=df, bmark=self._eh, start_date=ttm_start_date,
                                                      end_date=self._report_date,
                                                      trailing_period=12)
        #################################### 3y section #########################################
        three_y_start_date = self._report_date - relativedelta(years=3) + relativedelta(months=1)
        # allocations
        three_y_allocs = self.get_allocation_rpt(gcm=df, bmark=self._eh, start_date=three_y_start_date,
                                                     end_date=self._report_date)
        # get_standalone_return
        three_y_standalone_rtn = self.get_standalone_rtn_rpt(gcm=df, bmark=self._eh, start_date=three_y_start_date,
                                                                 end_date=self._report_date,
                                                                 trailing_period=36)
        # get_ctr
        three_y_ctr = self.get_ctr_rpt(gcm=df, bmark=self._eh, start_date=three_y_start_date,
                                           end_date=self._report_date, trailing_period=36)
        # get_attribution
        three_y_attrib = self.get_attribution_rpt(gcm=df, bmark=self._eh, start_date=three_y_start_date,
                                                      end_date=self._report_date,
                                                      trailing_period=36)

        # get excess table
        excess_return_total = self.get_excess_return_rpt(port_rtn=df, bmark_rtn=bmark_rtn)

        input_data = {
            "rtn_df": rtn_df,
            "beta_df": beta_df,
            "downside_df": downside_df,
            "correl_df": correl_df,
            "vol_df": vol_df,
            "sharpe_df": sharpe_df,
            "ytd_allocs": ytd_allocs,
            "ytd_standalone_rtn": ytd_standalone_rtn,
            "ytd_ctr": ytd_ctr,
            "ytd_attrib": ytd_attrib,
            "ttm_allocs": ttm_allocs,
            "ttm_standalone_rtn": ttm_standalone_rtn,
            "ttm_ctr": ttm_ctr,
            "ttm_attrib": ttm_attrib,
            "three_y_allocs": three_y_allocs,
            "three_y_standalone_rtn": three_y_standalone_rtn,
            "three_y_ctr": three_y_ctr,
            "three_y_attrib": three_y_attrib,
            "excess_return_total": excess_return_total
        }

        # old - replace with new datalake call
        # self._wb.save('{}{:%Y%m%d}_{}_BBA.xlsx'.format(self._output_dir,
        #                                                      (self._report_date.replace(day=1) + dt.timedelta(days=31)).replace(
        #                                                          day=1) - dt.timedelta(days=1),
        #                                                      acronym))



if __name__ == "__main__":
    report_date = dt.date(2022, 8, 1)
    runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.DataLake.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.PubDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.InvestmentsDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
            }
        },
    )
    # with Scenario(runner=runner, as_of_date=report_date).context():
    svc = BbaReport(
        runner=runner, report_date = report_date
    )

    acronyms = svc._gcm[svc._gcm.Date == svc._report_date].Acronym.unique()

    svc.generate_firmwide_report(acronyms)

    error_df = pd.DataFrame()
    for acronym in acronyms:
        try:
            svc.generate_portfolio_report(acronym=acronym)

        except Exception as e:
            error_msg = getattr(e, "message", repr(e))
            print(error_msg)
            error_df = pd.concat([pd.DataFrame(
                {
                    "Portfolio": [acronym],
                    "Date": [svc._report_date],
                    "ErrorMessage": [error_msg],
                }
            ), error_df])