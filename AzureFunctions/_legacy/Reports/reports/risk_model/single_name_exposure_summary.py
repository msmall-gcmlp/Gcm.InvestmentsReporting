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

    def get_single_nam_equity_exposure(self, as_of_date):
        exposure = self._runner.execute(
            params={
                "schema": "AnalyticsData",
                "table": "SingleNameEquityExposure",
                "operation": lambda query, item: query.filter(item.AsOfDate == as_of_date),
            },
            source=DaoSource.InvestmentsDwh,
            operation=lambda dao, params: dao.get_data(params),
        )

        exposure = pd.merge(self._all_holdings[['InvestmentGroupId', 'InvestmentGroupName']].drop_duplicates(),
                            exposure[['InvestmentGroupId', 'Issuer', 'Sector', 'AsOfDate', 'ExpNav']],
                            how='inner', on=['InvestmentGroupId'])
        return exposure

    def build_single_name_summary(self):
        single_name = self.get_single_nam_equity_exposure(
                                      as_of_date=self._as_of_date)

        single_name_overlaid = self._investment_group.overlay_singlename_exposure(single_name,
                                                                                  as_of_date=self._end_date)

        single_name_overlaid['Sector'] = single_name_overlaid.Sector.str.title()
        single_name_exposure = single_name_overlaid[['InvestmentGroupId', 'InvestmentGroupName', 'Issuer', 'Sector', 'ExpNav', 'AsOfDate']]
        single_name_exposure['AsOfDate'] = single_name_exposure['AsOfDate'].apply(lambda x: x.strftime('%Y-%m'))
        portfolio_level = pd.merge(self._all_holdings,
                                   single_name_exposure[['InvestmentGroupId', 'Issuer', 'Sector', 'ExpNav', 'AsOfDate']],
                                   how='inner',
                                   on=['AsOfDate', 'InvestmentGroupId'])
        portfolio_level['IssuerNav'] = portfolio_level['PctNav'] * portfolio_level['ExpNav']
        portfolio_level['Issuerbalance'] = (portfolio_level['OpeningBalance'] * portfolio_level['ExpNav']) / 1000

        # get funds  per portfolio
        portfolio_level = portfolio_level.groupby(['Acronym', 'Issuer', 'Sector'])['IssuerNav', 'Issuerbalance'].sum().reset_index()
        portfolio_level.sort_values(['Acronym'], ascending=True, inplace=True)
        group_porrtfolios = portfolio_level.groupby('Acronym', group_keys=False)
        largest_portfolio_level = group_porrtfolios.apply(lambda x: x.sort_values(by='IssuerNav', ascending=False).head(5))
        largest_portfolio_level = largest_portfolio_level[['Acronym', 'Issuer', 'Sector', 'Issuerbalance', 'IssuerNav']]
        # Firmwide
        firm_wide_holdings = self._investment_group.get_firmwide_allocation(
            start_date=self._start_date,
            end_date=self._end_date,)
        firm_wide_holdings['AsOfDate'] = firm_wide_holdings['Date'].apply(lambda x: x.strftime('%Y-%m'))
        total_balance = firm_wide_holdings['OpeningBalance'].sum()
        firm_wide_holdings['PctNav'] = firm_wide_holdings['OpeningBalance'] / total_balance
        firm_wide = pd.merge(single_name_exposure[['InvestmentGroupId', 'Issuer', 'Sector', 'ExpNav', 'AsOfDate']],
                             firm_wide_holdings,
                             how='inner',
                             on=['InvestmentGroupId', 'AsOfDate'])
        firm_wide['firmwide_pct'] = firm_wide['ExpNav'] * firm_wide['PctNav']
        firm_wide['firmwide_allocation'] = (firm_wide['ExpNav'] * firm_wide['OpeningBalance']) / 1000

        firm_wide['IssuerSum'] = firm_wide.groupby(['AsOfDate', 'Issuer'])['firmwide_pct'].transform(
            'sum')
        firm_wide['Issuer_allocation'] = firm_wide.groupby(['AsOfDate', 'Issuer'])['firmwide_allocation'].transform(
            'sum')
        firm_wide.sort_values(['IssuerSum', 'firmwide_pct'], ascending=False, inplace=True)
        firm_wide = firm_wide[firm_wide['InvestmentGroupName'] != 'Atlas Enhanced Fund']
        largest_firm_wide_level = firm_wide.groupby(['Issuer']).head(3).reset_index(drop=True)
        largest_firm_wide_level = largest_firm_wide_level.groupby(['Issuer', 'Sector', 'Issuer_allocation', 'IssuerSum'])['InvestmentGroupName'].agg(
            ', '.join).reset_index()
        largest_firm_wide_level = largest_firm_wide_level[['Issuer', 'InvestmentGroupName', 'Sector', 'Issuer_allocation', 'IssuerSum']]
        largest_firm_wide_level.sort_values(['IssuerSum'], ascending=False, inplace=True)
        return [largest_portfolio_level, largest_firm_wide_level]

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

        firm_wide_longs = firm_wide[firm_wide['IssuerSum'] > 0.0]
        firm_wide_shorts = firm_wide[firm_wide['IssuerSum'] <= 0.0]
        firm_wide_shorts.sort_values(by='IssuerSum', ascending=True, inplace=True)
        portfolio_max_row = 7 + portfolio.shape[0]
        portfolio_max_column = 'F'
        print_area_portfolio = {'PortfolioAllocation': 'B1:' + portfolio_max_column + str(portfolio_max_row)}
        input_data_portfolio = {
            "as_of_date": as_of_date1,
            "portfolio_allocation": portfolio,

        }
        input_data_issuer = {
            "as_of_date1": as_of_date1,
            "as_of_date2": as_of_date1,
            "firmwide_allocation_longs": firm_wide_longs,
            "firmwide_allocation_shorts": firm_wide_shorts
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

    end_date = dt.date(2022, 9, 30)

    with Scenario(dao=runner, as_of_date=end_date).context():
        SingleNameEquityExposureSummary().execute()
