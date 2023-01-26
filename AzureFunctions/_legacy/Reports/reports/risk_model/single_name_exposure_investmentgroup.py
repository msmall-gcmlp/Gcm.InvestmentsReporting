import datetime
import datetime as dt
from functools import cached_property
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs
import pandas as pd
from gcm.Dao.DaoSources import DaoSource
from sqlalchemy import func

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


class SingleNameInvestmentGroupReport(ReportingRunnerBase):
    def __init__(self):
        super().__init__(runner=Scenario.get_attribute("dao"))
        self._as_of_date = Scenario.get_attribute("as_of_date")
        self._end_date = self._as_of_date
        self._start_date = datetime.date(self._as_of_date.year, self._as_of_date.month, 1)
        self.__investment_group = None
        self._inv_group_ids = None
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
    def _all_inv_group_ids(self):
        # dimn = self._portfolio.get_dimensions()
        holdings = self._all_holdings['InvestmentGroupId'].unique()
        investment_group_ids = holdings.astype('float').astype('int').tolist()
        return investment_group_ids

    @property
    def _investment_group(self):
        self.__investment_group = InvestmentGroup(investment_group_ids=self._inv_group_ids)
        return self.__investment_group

    @property
    def _pub_investment_group(self):
        holdings = self._all_holdings[self._all_holdings['InvestmentGroupId'] == self._inv_group_ids]
        pub_investment_group_id = holdings['PubInvestmentGroupId'].unique()
        pub_investment_group_id = pub_investment_group_id.astype('float').astype('int').tolist()
        return pub_investment_group_id

    @property
    def _get_investment_group_name(self):
        holdings = self._all_holdings[self._all_holdings['InvestmentGroupId'] == self._inv_group_ids]
        investment_group_name = holdings.InvestmentGroupName.unique()
        return investment_group_name

    def get_single_name_equityexposure(self, investment_group_id, as_of_date):

        def _create_query(session, items):
            query = session.query(
                items["AnalyticsData.SingleNameEquityExposure"]
            ).select_from(items["AnalyticsData.SingleNameEquityExposure"])

            return query

        def _build_filters(session, items, investment_group_id, as_of_date):
            query = _create_query(session, items)

            subq = (
                session.query(
                    func.max(items["AnalyticsData.SingleNameEquityExposure"].AsOfDate).label("maxdate"),
                ).filter(
                    items["AnalyticsData.SingleNameEquityExposure"].InvestmentGroupId == investment_group_id,
                    items["AnalyticsData.SingleNameEquityExposure"].AsOfDate <= as_of_date
                ).subquery("t2")
            )

            query = query.filter(
                items["AnalyticsData.SingleNameEquityExposure"].InvestmentGroupId == investment_group_id,
                items["AnalyticsData.SingleNameEquityExposure"].AsOfDate == subq.c.maxdate
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
        result['InvestmentGroupName'] = self._get_investment_group_name[0]
        return result

    def build_single_name(self):
        # exposure by investments groups will reflect all the latest data available in SingleNameEquityExposure
        # table
        single_name = self.get_single_name_equityexposure(investment_group_id=self._inv_group_ids,
                                                          as_of_date=self._as_of_date)

        single_name_overlaid = self._investment_group.overlay_singlename_exposure(single_name,
                                                                                  as_of_date=self._end_date)
        single_name_overlaid['Sector'] = single_name_overlaid.Sector.str.title()
        #  assign sector to 'other' if there are duplicated sectors
        dupliacted_Issuers = single_name_overlaid[single_name_overlaid[['Issuer']].duplicated()]['Issuer']
        single_name_overlaid.loc[single_name_overlaid['Issuer'].isin(dupliacted_Issuers.to_list()), 'Sector'] = 'Other'
        single_name_overlaid.sort_values(['ExpNav'], ascending=False, inplace=True)
        return single_name_overlaid[['Issuer', 'Sector', 'ExpNav', 'AsOfDate']]

    def get_header_info(self):
        return pd.DataFrame(
            {
                "header_info": [
                    self._get_investment_group_name[0],
                    'ARS',
                    self._as_of_date,
                ]
            })

    def generate_single_name_report(self, investment_group_id):
        self._inv_group_ids = investment_group_id
        header_info = self.get_header_info()
        single_name = self.build_single_name()
        if single_name.empty:
            return
        single_name_max_row = 7 + single_name.shape[0]
        single_name_max_column = 'E'
        print_areas = {'ManagerAllocation': 'B1:' + single_name_max_column + str(single_name_max_row),
                       }
        input_data = {
            "header": header_info,
            "manager_allocation": single_name,

        }

        as_of_date = dt.datetime.combine(self._as_of_date, dt.datetime.min.time())
        with Scenario(as_of_date=as_of_date).context():
            InvestmentsReportRunner().execute(
                data=input_data,
                template="SingleNameExposure_Template_InvestmentGroup.xlsx",
                save=True,
                save_as_pdf=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.manager_fund_group,
                entity_name=self._get_investment_group_name[0].replace("/", ""),
                entity_display_name=self._get_investment_group_name[0].replace("/", ""),
                entity_ids=self._pub_investment_group,
                entity_source=DaoSource.PubDwh,
                report_name="ARS Single Name Equity Exposure",
                report_type=ReportType.Risk,
                aggregate_intervals=AggregateInterval.MTD,
                report_frequency="Monthly",
                print_areas=print_areas
            )

    def run(self, **kwargs):
        error_df = pd.DataFrame()
        self._all_inv_group_ids.sort()
        for ing_group_ids in self._all_inv_group_ids:
            error_msg = 'success'
            if ing_group_ids == 9077:
                pass
            self._inv_group_ids = ing_group_ids
            try:
                self.generate_single_name_report(investment_group_id=self._inv_group_ids)

            except Exception as e:
                error_msg = getattr(e, "message", repr(e))
                error_df = pd.concat(
                    [
                        pd.DataFrame(
                            {
                                "Portfolio": [ing_group_ids],
                                "Date": [self._as_of_date],
                                "ErrorMessage": [error_msg],
                            }
                        ),
                        error_df,
                    ]
                )
        return str(ing_group_ids) + " " + error_msg


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
        SingleNameInvestmentGroupReport().execute()
