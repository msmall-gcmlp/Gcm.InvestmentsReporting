import datetime
import datetime as dt
from functools import cached_property
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs
import pandas as pd
from gcm.Dao.DaoSources import DaoSource
from _legacy.core.ReportStructure.report_structure import (
    ReportingEntityTypes,
    ReportType,
    AggregateInterval,
)
from _legacy.core.Runners.investmentsreporting import (
    InvestmentsReportRunner,
)
from _legacy.core.reporting_runner_base import (
    ReportingRunnerBase,
)
from gcm.inv.dataprovider.portfolio import Portfolio
from gcm.inv.dataprovider.investment_group import InvestmentGroup
from gcm.inv.scenario import Scenario


class SingleNamePortfolioReport(ReportingRunnerBase):
    def __init__(self):
        super().__init__(runner=Scenario.get_attribute("dao"))
        self._as_of_date = Scenario.get_attribute("as_of_date")
        self._end_date = self._as_of_date
        self._start_date = datetime.date(self._as_of_date.year, self._as_of_date.month, 1)
        self.__investment_group = None
        self.__all_holdings = None
        self._portfolio = Portfolio()
        self._portfolio_acronym = None
        self.__all_pub_port_dimn = None

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
    def _portfolio_holdings(self):
        return self._all_holdings[self._all_holdings.Acronym == self._portfolio_acronym]

    @property
    def _all_acronyms(self):
        return self._all_holdings.Acronym.unique()

    @property
    def _selected_acronyms(self):
        if self._portfolio_acronym is None:
            return self._all_acronyms
        else:
            return self._portfolio_acronym

    @property
    def _inv_group_ids(self):
        holdings = self._portfolio_holdings['InvestmentGroupId'].unique()
        investment_group_ids = holdings.astype('float').astype('int').tolist()
        return investment_group_ids

    @cached_property
    def _all_pub_port_dimn(self):
        if self.__all_pub_port_dimn is None:
            dimn = self._portfolio.get_dimensions()
            self.__all_pub_port_dimn = dimn[["Acronym", "MasterId"]].rename(columns={"MasterId": "PubPortfolioId"})
        return self.__all_pub_port_dimn

    @property
    def _investment_group(self):
        if self.__investment_group is None:
            self.__investment_group = InvestmentGroup(investment_group_ids=self._inv_group_ids)
        return self.__investment_group

    @property
    def _pub_portfolio_id(self):
        port_dimn = self._all_pub_port_dimn[self.__all_pub_port_dimn["Acronym"] == self._portfolio_acronym]
        return port_dimn["PubPortfolioId"].squeeze()

    def get_single_nam_equity_exposure(self, investment_group_id, as_of_date):
        exposure = self._runner.execute(
            params={
                "schema": "AnalyticsData",
                "table": "SingleNameEquityExposure",
                "operation": lambda query, item: query.filter(item.InvestmentGroupId.in_(investment_group_id),
                                                              item.AsOfDate == as_of_date),
            },
            source=DaoSource.InvestmentsDwh,
            operation=lambda dao, params: dao.get_data(params),
        )

        exposure = pd.merge(self._portfolio_holdings[['InvestmentGroupId', 'InvestmentGroupName']].drop_duplicates(),
                            exposure[['InvestmentGroupId', 'Issuer', 'Sector', 'AsOfDate', 'ExpNav']],
                            how='inner', on=['InvestmentGroupId'])
        return exposure

    def build_single_name(self):
        single_name = self.get_single_nam_equity_exposure(investment_group_id=self._inv_group_ids, as_of_date=self._as_of_date)
        single_name = self._investment_group.overlay_singlename_exposure(single_name, as_of_date=self._end_date)
        single_name['AsOfDate'] = single_name['AsOfDate'].apply(lambda x: x.strftime('%Y-%m'))
        portfolio_level = pd.merge(self._portfolio_holdings,
                                   single_name[['InvestmentGroupId', 'Issuer', 'Sector', 'ExpNav', 'AsOfDate']],
                                   how='inner',
                                   on=['AsOfDate', 'InvestmentGroupId'])
        portfolio_level['PortfolioNav'] = portfolio_level['PctNav'] * portfolio_level['ExpNav']

        # get funds without exposure
        portfolio_level['IssuerSum'] = portfolio_level.groupby(['AsOfDate', 'Issuer'])['PortfolioNav'].transform(
            'sum')
        groupped = portfolio_level.groupby(['AsOfDate', 'Issuer', 'InvestmentGroupName', 'IssuerSum', 'Sector', 'ExpNav'])[
            'PortfolioNav'].sum().reset_index()
        groupped.sort_values(['IssuerSum', 'PortfolioNav'], ascending=False, inplace=True)
        groupped['Issuer'] = groupped['Issuer'].str[0:39]
        return groupped[['InvestmentGroupName', 'Issuer', 'PortfolioNav', 'IssuerSum', 'Sector', 'ExpNav']]

    def get_header_info(self):
        return pd.DataFrame(
            {
                "header_info": [
                    self._portfolio_acronym,
                    self._as_of_date,
                ]
            })

    def generate_single_name_report(self, acronym):
        self._portfolio_acronym = acronym
        header_info = self.get_header_info()
        single_name = self.build_single_name()
        single_name = pd.merge(single_name, self._portfolio_holdings[['InvestmentGroupName', 'OpeningBalance', 'PctNav']],
                               how='left', on='InvestmentGroupName')
        single_name.rename(columns={'ExpNav': 'manager_allocation_pct'}, inplace=True)
        single_name['Sector'] = single_name.Sector.str.title()
        portfolio_balance = self._portfolio_holdings[['OpeningBalance']].sum()
        single_name['portfolio_allocation'] = (portfolio_balance[0] * single_name['IssuerSum']) / 1000
        single_name['portfolio_allocation_permanager'] = (portfolio_balance[0] * single_name['PortfolioNav']) / 1000
        manager_allocation = single_name[['Issuer', 'InvestmentGroupName',
                                          'portfolio_allocation_permanager', 'PortfolioNav', 'manager_allocation_pct']].drop_duplicates()
        manager_allocation = manager_allocation[manager_allocation['InvestmentGroupName'] != 'Atlas Enhanced Fund']
        portfolio_allocation = single_name[['Issuer', 'Sector', 'portfolio_allocation', 'IssuerSum']].drop_duplicates()
        dupliacted_Issuers = portfolio_allocation[portfolio_allocation[['Issuer']].duplicated()]['Issuer']
        portfolio_allocation.loc[portfolio_allocation['Issuer'].isin(dupliacted_Issuers.to_list()), 'Sector'] = 'Other'
        portfolio_allocation.drop_duplicates(subset='Issuer', inplace=True)
        excluded_managers = self._portfolio_holdings[~ self._portfolio_holdings['InvestmentGroupName'].isin(single_name['InvestmentGroupName'].to_list())]
        excluded_managers['OpeningBalance'] = excluded_managers['OpeningBalance'] / 1000
        manager_allocation_longs = manager_allocation[manager_allocation['manager_allocation_pct'] > 0.0]
        manager_allocation_shorts = manager_allocation[manager_allocation['manager_allocation_pct'] <= 0.0]
        manager_allocation_shorts.sort_values(by='manager_allocation_pct', ascending=True, inplace=True)
        portfolio_allocation_longs = portfolio_allocation[portfolio_allocation['IssuerSum'] > 0.0]
        portfolio_allocation_shorts = portfolio_allocation[portfolio_allocation['IssuerSum'] <= 0.0]
        portfolio_allocation_shorts.sort_values(by='IssuerSum', ascending=True, inplace=True)
        excluded_max_row = 10 + excluded_managers.shape[0]
        excludedr_max_column = 'D'
        print_areas = {
                       'ExcludedManagers': 'B1:' + excludedr_max_column + str(excluded_max_row)}
        input_data = {
            "header_info_1": header_info,
            "header_info_2": header_info,
            "header_info_3": header_info,
            "header_info_4": header_info,
            "header_info_5": header_info,
            "manager_allocation_longs": manager_allocation_longs,
            "manager_allocation_shorts": manager_allocation_shorts,
            "portfolio_allocation_longs": portfolio_allocation_longs,
            "portfolio_allocation_shorts": portfolio_allocation_shorts,
            "excluded_managers": excluded_managers[['InvestmentGroupName', 'OpeningBalance', 'PctNav']],

        }

        as_of_date = dt.datetime.combine(self._as_of_date, dt.datetime.min.time())
        with Scenario(as_of_date=as_of_date).context():
            InvestmentsReportRunner().execute(
                data=input_data,
                template="SingleNamePosition_Template_Portfolio.xlsx",
                save=True,
                save_as_pdf=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.portfolio,
                entity_name=self._portfolio_acronym,
                entity_display_name=self._portfolio_acronym,
                entity_ids=[self._pub_portfolio_id.item()],
                entity_source=DaoSource.PubDwh,
                report_name="ARS Single Name Position - Portfolio",
                report_type=ReportType.Risk,
                aggregate_intervals=AggregateInterval.MTD,
                report_frequency="Monthly",
                print_areas=print_areas
            )

    def run(self, **kwargs):
        error_df = pd.DataFrame()
        self._selected_acronyms.sort()
        for acronym in self._selected_acronyms:
            error_msg = 'success'
            self._portfolio_acronym = acronym
            try:
                self.generate_single_name_report(acronym=acronym)

            except Exception as e:
                error_msg = getattr(e, "message", repr(e))
                error_df = pd.concat(
                    [
                        pd.DataFrame(
                            {
                                "Portfolio": [acronym],
                                "Date": [self._as_of_date],
                                "ErrorMessage": [error_msg],
                            }
                        ),
                        error_df,
                    ]
                )
        return acronym + error_msg


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

    end_date = dt.date(2022, 11, 30)

    with Scenario(dao=runner, as_of_date=end_date).context():
        SingleNamePortfolioReport().execute()