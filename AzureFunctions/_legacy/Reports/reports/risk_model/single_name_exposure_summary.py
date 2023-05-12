import datetime
import datetime as dt
from functools import cached_property
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs
import pandas as pd
from gcm.Dao.DaoSources import DaoSource
from _legacy.core.Runners.investmentsreporting import InvestmentsReportRunner
from _legacy.core.reporting_runner_base import (
    ReportingRunnerBase,
)
from gcm.inv.dataprovider.portfolio import Portfolio
from gcm.inv.dataprovider.investment_group import InvestmentGroup
from gcm.inv.scenario import Scenario
from _legacy.core.ReportStructure.report_structure import (
    ReportingEntityTypes,
    ReportType,
    ReportVertical,
    AggregateInterval,
)
from sqlalchemy import func, and_
from gcm.inv.dataprovider.utilities import filter_many
# TODO: Make sure to change back to prd
runner_prd = DaoRunner(
    container_lambda=lambda b, i: b.config.from_dict(i),
    config_params={
        DaoRunnerConfigArgs.dao_global_envs.name: {
            DaoSource.InvestmentsDwh.name: {
                "Environment": "prd",
                "Subscription": "prd",
            },
        }
    },
)


class SingleNameEquityExposureSummary(ReportingRunnerBase):
    def __init__(self):
        super().__init__(runner=Scenario.get_attribute("dao"))
        self._as_of_date = Scenario.get_attribute("as_of_date")
        self._end_date = self._as_of_date
        self._start_date = datetime.date(self._as_of_date.year, self._as_of_date.month, 1)
        self.__investment_group = None
        self.__all_holdings = None
        self._portfolio = Portfolio()

    @cached_property
    def _all_holdings(self):
        if self.__all_holdings is None:
            holdings = self._portfolio.get_holdings(
                start_date=self._start_date,
                end_date=self._end_date,
                lookthrough='True'
            )
            holdings['AsOfDate'] = holdings['Date'].apply(lambda x: x.strftime('%Y-%m'))
            self.__all_holdings = holdings
        return self.__all_holdings

    @property
    def _inv_group_ids(self):
        holdings = self._all_holdings['InvestmentGroupId'].unique()
        investment_group_ids = holdings.astype('float').astype('int').tolist()
        return investment_group_ids

    @property
    def _investment_group(self):
        if self.__investment_group is None:
            self.__investment_group = InvestmentGroup(investment_group_ids=self._inv_group_ids)
        return self.__investment_group

    # def get_single_nam_equity_exposure(self, as_of_date):
    #     exposure = self._runner.execute(
    #         params={
    #             "schema": "AnalyticsData",
    #             "table": "SingleNameEquityExposure",
    #             "operation": lambda query, item: query.filter(item.AsOfDate == as_of_date),
    #         },
    #         source=DaoSource.InvestmentsDwh,
    #         operation=lambda dao, params: dao.get_data(params),
    #     )
    #
    #     exposure = pd.merge(self._all_holdings[['InvestmentGroupId', 'InvestmentGroupName']].drop_duplicates(),
    #                         exposure[['InvestmentGroupId', 'Issuer', 'Sector', 'AsOfDate', 'ExpNav']],
    #                         how='inner', on=['InvestmentGroupId'])
    #     return exposure

    def get_single_nam_equity_exposure(self, investment_group_id, as_of_date):

        def _create_query(session, items):
            query = session.query(
                items["AnalyticsData.SingleNameEquityExposure"]
            ).select_from(items["AnalyticsData.SingleNameEquityExposure"])

            return query

        def _build_filters(session, items, investment_group_id, as_of_date):
            query = _create_query(session, items)
            query = filter_many(
                query=query,
                items=items,
                table_name="AnalyticsData.SingleNameEquityExposure",
                col_name="InvestmentGroupId",
                ids=investment_group_id,
            )
            subq = (
                session.query(
                    func.max(items["AnalyticsData.SingleNameEquityExposure"].AsOfDate).label("maxdate"),
                    items["AnalyticsData.SingleNameEquityExposure"].InvestmentGroupId,
                ).group_by(
                    items["AnalyticsData.SingleNameEquityExposure"].InvestmentGroupId,
                ).filter(
                    items["AnalyticsData.SingleNameEquityExposure"].AsOfDate <= as_of_date,
                ).subquery("t2")
            )

            query = query.join(
                subq,
                and_(
                    items["AnalyticsData.SingleNameEquityExposure"].InvestmentGroupId == subq.c.InvestmentGroupId,
                    items["AnalyticsData.SingleNameEquityExposure"].AsOfDate == subq.c.maxdate,
                ),
            )

            return query

        result = self._runner.execute(
            params={
                "tables": [
                    {
                        "schema": "AnalyticsData",
                        "table": "SingleNameEquityExposure",
                    },

                ],
                "operation": lambda session, items: _build_filters(session, items,
                                                                   investment_group_id=investment_group_id,
                                                                   as_of_date=as_of_date),
            },
            source=DaoSource.InvestmentsDwh,
            operation=lambda dao, params: dao.get_data(params),
        )
        result = pd.merge(self._all_holdings[['InvestmentGroupId', 'InvestmentGroupName']].drop_duplicates(),
                          result[['InvestmentGroupId', 'Issuer', 'AssetClass', 'Sector', 'AsOfDate', 'ExpNav']],
                          how='inner', on=['InvestmentGroupId'])
        return result

    def get_vol(self, as_of_date, runner):

        def _create_query(session, items):
            return session.query(
                items["AnalyticsData.SecurityStandaloneRisk"].HoldingDate.label("Date"),
                items["AnalyticsData.SecurityStandaloneRisk"].SecurityId,
                items["AnalyticsData.SecurityStandaloneRisk"].TotalRisk,
                items["AnalyticsData.SecurityStandaloneRisk"].SpecificResidualRisk,
                items["AnalyticsData.SecurityDimn"].Name.label("SecurityName"),
                items["AnalyticsData.IssuerDimn"].Name.label("Issuer"),
            ).select_from(items["AnalyticsData.SecurityStandaloneRisk"])

        def _build_filters(session, items, as_of_date):
            query = _create_query(session, items)

            query = query.join(
                items["AnalyticsData.SecurityDimn"],
                and_(
                    items["AnalyticsData.SecurityStandaloneRisk"].SecurityId == items[
                        "AnalyticsData.SecurityDimn"].Id,
                ),
                isouter=True,
            )
            query = query.join(
                items["AnalyticsData.IssuerDimn"],
                and_(
                    items["AnalyticsData.SecurityDimn"].IssuerId == items[
                        "AnalyticsData.IssuerDimn"].Id,
                ),
                isouter=True,
            )
            query = query.filter(items["AnalyticsData.SecurityStandaloneRisk"].HoldingDate == as_of_date)
            query = query.filter(items["AnalyticsData.SecurityDimn"].InstrumentType == 'Equity Security')
            return query

        result = runner.execute(
            params={
                "tables": [
                    {
                        "schema": "AnalyticsData",
                        "table": "SecurityStandaloneRisk",
                    },
                    {
                        "schema": "AnalyticsData",
                        "table": "SecurityDimn",
                    },
                    {
                        "schema": "AnalyticsData",
                        "table": "IssuerDimn",
                    }

                ],
                "operation": lambda session, items: _build_filters(session, items,
                                                                   as_of_date=as_of_date),
            },
            source=DaoSource.InvestmentsDwh,
            operation=lambda dao, params: dao.get_data(params),
        )
        return result

    def build_single_name_summary(self, usage_limit=0.03):
        single_name = self.get_single_nam_equity_exposure(investment_group_id=self._inv_group_ids,
                                                          as_of_date=self._as_of_date)

        single_name_overlaid = self._investment_group.overlay_singlename_exposure(single_name,
                                                                                  as_of_date=self._end_date)

        single_name_overlaid['Sector'] = single_name_overlaid.Sector.str.title()
        single_name_exposure = single_name_overlaid[['InvestmentGroupId', 'InvestmentGroupName', 'Issuer', 'Sector','AssetClass' ,'ExpNav', 'AsOfDate']]
        # single_name_exposure['AsOfDate'] = single_name_exposure['AsOfDate'].apply(lambda x: x.strftime('%Y-%m'))
        portfolio_level = pd.merge(self._all_holdings,
                                   single_name_exposure[['InvestmentGroupId', 'Issuer', 'Sector', 'ExpNav', 'AsOfDate','AssetClass']],
                                   how='inner',
                                   on=['InvestmentGroupId'])
        portfolio_level['IssuerNav'] = portfolio_level['PctNav'] * portfolio_level['ExpNav']
        portfolio_level['Issuerbalance'] = (portfolio_level['OpeningBalance'] * portfolio_level['ExpNav']) / 1000

        # get funds  per portfolio
        portfolio_level = portfolio_level[['Acronym', 'Issuer', 'Sector', 'AssetClass',
                                           'IssuerNav', 'Issuerbalance']].groupby(['Acronym', 'Issuer', 'Sector','AssetClass']).sum().reset_index()
        portfolio_level.sort_values(['Acronym', 'IssuerNav'], ascending=[True, False], inplace=True)
        # group_porrtfolios = portfolio_level.groupby(portfolio_level['Acronym'])
        #(portfolio_level['IssuerNav'] >= 0.015).groupby(portfolio_level['Acronym'])
        #group_porrtfolios = portfolio_level.groupby('Acronym', group_keys=False)
        portfolio_level.loc[:, 'Usage'] = abs(portfolio_level['IssuerNav']) / usage_limit
        portfolio_level.loc[portfolio_level['Sector'].str.contains('Privates'), 'Usage'] = None

        largest_portfolio_level_long = portfolio_level[portfolio_level['IssuerNav'] >= 0.015]

        # largest_portfolio_level_long = group_porrtfolios.apply(lambda x: x.sort_values(by='IssuerNav', ascending=False).head(5))
        # largest_portfolio_level_long = group_porrtfolios[group_porrtfolios['IssuerNav'] >= 0.015]
        largest_portfolio_level_long = largest_portfolio_level_long[['Acronym', 'Issuer', 'AssetClass', 'Sector', 'Issuerbalance', 'IssuerNav', 'Usage']]
        # largest_portfolio_level_short = group_porrtfolios.apply(
        #     lambda x: x.sort_values(by='IssuerNav', ascending=True).head(5))
        largest_portfolio_level_short = portfolio_level[portfolio_level['IssuerNav'] <= -0.015]
        largest_portfolio_level_short = largest_portfolio_level_short[
            ['Acronym', 'Issuer', 'AssetClass', 'Sector', 'Issuerbalance', 'IssuerNav', 'Usage']]

        # Firmwide
        firm_wide_holdings = self._investment_group.get_firmwide_allocation(
            start_date=self._start_date,
            end_date=self._end_date,)
        firm_wide_holdings['AsOfDate'] = firm_wide_holdings['Date'].apply(lambda x: x.strftime('%Y-%m'))
        total_balance = firm_wide_holdings['OpeningBalance'].sum()

        firm_wide_holdings['PctNav'] = firm_wide_holdings['OpeningBalance'] / total_balance
        firm_wide = pd.merge(single_name_exposure[['InvestmentGroupId', 'Issuer', 'Sector', 'AssetClass', 'ExpNav']],
                             firm_wide_holdings,
                             how='inner',
                             on=['InvestmentGroupId'])
        firm_wide['firmwide_pct'] = firm_wide['ExpNav'] * firm_wide['PctNav']
        firm_wide['firmwide_allocation'] = (firm_wide['ExpNav'] * firm_wide['OpeningBalance']) / 1000

        firm_wide['IssuerSum'] = firm_wide.groupby(['Issuer'])['firmwide_pct'].transform(
            'sum')
        firm_wide['Issuer_allocation'] = firm_wide.groupby(['Issuer'])['firmwide_allocation'].transform(
            'sum')
        firm_wide.sort_values(['IssuerSum', 'firmwide_pct'], ascending=False, inplace=True)
        firm_wide = firm_wide[firm_wide['InvestmentGroupName'] != 'Atlas Enhanced Fund']
        largest_firm_wide_level = firm_wide.groupby(['Issuer']).head(3).reset_index(drop=True)
        largest_firm_wide_level = largest_firm_wide_level.groupby(['Issuer', 'Sector', 'AssetClass', 'Issuer_allocation', 'IssuerSum'])['InvestmentGroupName'].agg(
            ', '.join).reset_index()
        vol = self._investment_group.normalize_issuers(self.get_vol(as_of_date=self._as_of_date, runner=runner_prd))
        vol['TotalRisk'] = vol['TotalRisk'] / 100
        vol['SpecificResidualRisk'] = vol['SpecificResidualRisk'] / 100
        vol = vol[['TotalRisk', 'SpecificResidualRisk', 'Issuer']].groupby(['Issuer']).mean().reset_index()
        largest_firm_wide_level = pd.merge(largest_firm_wide_level, vol[['TotalRisk', 'SpecificResidualRisk', 'Issuer']], how='left', on='Issuer')
        largest_firm_wide_level = largest_firm_wide_level[['Issuer', 'AssetClass', 'InvestmentGroupName', 'Sector',
                                                           'TotalRisk', 'SpecificResidualRisk', 'Issuer_allocation',
                                                           'IssuerSum']]
        largest_firm_wide_level.sort_values(['IssuerSum'], ascending=False, inplace=True)
        return [largest_portfolio_level_long, largest_firm_wide_level, largest_portfolio_level_short]

    def get_as_of_date(self):
        return pd.DataFrame(
            {
                "as_of_date1": [

                    self._as_of_date,
                ]
            })

    def generate_single_name_summary_report(self):
        as_of_date1 = self.get_as_of_date()
        single_name = self.build_single_name_summary()
        firm_wide = single_name[1]
        firm_wide['InvestmentGroupName'] = firm_wide['InvestmentGroupName'].str[0:46]
        firm_wide['Issuer'] = firm_wide['Issuer'].str[0:39]
        portfolio = single_name[0]
        portfolio['Issuer'] = portfolio['Issuer'].str[0:39]
        portfolio_long_equity = portfolio[portfolio['AssetClass'] == 'Equity'].drop(columns=['AssetClass'])
        portfolio_long_credit = portfolio[portfolio['AssetClass'] == 'Credit'].drop(columns=['AssetClass'])
        portfolio_short = single_name[2]
        portfolio_short['Issuer'] = portfolio_short['Issuer'].str[0:39]
        portfolio_short_equity = portfolio_short[portfolio_short['AssetClass'] == 'Equity'].drop(columns=['AssetClass'])
        portfolio_short_credit = portfolio_short[portfolio_short['AssetClass'] == 'Credit'].drop(columns=['AssetClass'])

        firm_wide_longs = firm_wide[firm_wide['IssuerSum'] > 0.0]
        firm_wide_longs_equity = firm_wide_longs[firm_wide_longs['AssetClass'] == 'Equity'].drop(columns=['AssetClass'])
        firm_wide_longs_credit = firm_wide_longs[firm_wide_longs['AssetClass'] == 'Credit'].drop(columns=['AssetClass'])
        firm_wide_shorts = firm_wide[firm_wide['IssuerSum'] <= 0.0]
        firm_wide_shorts_equity = firm_wide_shorts[firm_wide_shorts['AssetClass'] == 'Equity'].drop(columns=['AssetClass'])
        firm_wide_shorts_credit = firm_wide_shorts[firm_wide_shorts['AssetClass'] == 'Credit'].drop(columns=['AssetClass'])
        firm_wide_shorts_equity.sort_values(by='IssuerSum', ascending=True, inplace=True)
        firm_wide_shorts_credit.sort_values(by='IssuerSum', ascending=True, inplace=True)

        portfolio_max_row_long_equity = 9 + portfolio_long_equity.shape[0]
        portfolio_max_row_long_credit = 9 + portfolio_long_credit.shape[0]
        portfolio_short_max_row_equity = 9 + portfolio_short_equity.shape[0]
        portfolio_short_max_row_credit = 9 + portfolio_short_credit.shape[0]
        portfolio_max_column = 'G'

        portfolio_short__max_column = 'G'
        print_area_portfolio = {'PortfolioAllocationLong': 'B1:' + portfolio_max_column + str(portfolio_max_row_long_equity),
                                'PortfolioAllocationShort': 'B1:' + portfolio_short__max_column + str(portfolio_max_row_long_credit),
                                'PortfolioAllocationCredLong': 'B1:' + portfolio_max_column + str(portfolio_short_max_row_equity),
                                'PortfolioAllocationCreditShort': 'B1:' + portfolio_short__max_column + str(portfolio_short_max_row_credit)
                                }
        input_data_portfolio = {
            "as_of_date_long": as_of_date1,
            "as_of_date_long_credit": as_of_date1,
            "as_of_date_short": as_of_date1,
            "as_of_date_short_credit": as_of_date1,
            "portfolio_allocationlong_equity": portfolio_long_equity,
            "portfolio_allocationlong_credit": portfolio_long_credit,
            "portfolio_allocationshort_equity": portfolio_short_equity,
            "portfolio_allocationshort_credit": portfolio_short_credit
        }
        input_data_issuer = {
            "as_of_date1": as_of_date1,
            "as_of_date2": as_of_date1,
            "as_of_date11": as_of_date1,
            "as_of_date22": as_of_date1,
            "firmwide_allocation_longs": firm_wide_longs_equity,
            "firmwide_allocation_credit_longs": firm_wide_longs_credit,
            "firmwide_allocation_shorts": firm_wide_shorts_equity,
            "firmwide_allocation_credit_shorts": firm_wide_shorts_credit,
        }
        as_of_date = dt.datetime.combine(self._as_of_date, dt.datetime.min.time())
        with Scenario(as_of_date=as_of_date).context():
            InvestmentsReportRunner().execute(
                data=input_data_issuer,
                template="SingleNamePosition_Template_Firm_Issuer.xlsx",
                save=True,
                save_as_pdf=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.cross_entity,
                entity_name='FIRM',
                entity_source=DaoSource.PubDwh,
                report_name="ARS Single Name Position - Firm x Issuer",
                report_type=ReportType.Risk,
                report_frequency="Monthly",
                report_vertical=ReportVertical.ARS,
                aggregate_intervals=AggregateInterval.MTD,
            )
            InvestmentsReportRunner().execute(
                data=input_data_portfolio,
                template="SingleNamePosition_Template_Firm_Portfolio.xlsx",
                save=True,
                save_as_pdf=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.cross_entity,
                entity_name='FIRM',
                entity_source=DaoSource.PubDwh,
                report_name="ARS Single Name Position - Firm x Portfolio",
                report_type=ReportType.Risk,
                report_frequency="Monthly",
                report_vertical=ReportVertical.ARS,
                aggregate_intervals=AggregateInterval.MTD,
                print_areas=print_area_portfolio)

    def run(self, **kwargs):
        self.generate_single_name_summary_report()
        return 'success'


if __name__ == "__main__":
    runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.InvestmentsDwh.name: {
                    "Environment": "dev",
                    "Subscription": "nonprd",
                },
                DaoSource.PubDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.ReportingStorage.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
            }
        },
    )

    end_date = dt.date(2023, 2, 28)

    with Scenario(dao=runner, as_of_date=end_date).context():
        SingleNameEquityExposureSummary().execute()
