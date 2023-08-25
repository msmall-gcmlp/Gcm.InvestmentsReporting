import datetime
import datetime as dt
import os
from functools import cached_property
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs
import pandas as pd
from gcm.Dao.DaoSources import DaoSource

from _legacy.core.reporting_runner_base import (
    ReportingRunnerBase,
)
from gcm.inv.dataprovider.portfolio import Portfolio
from gcm.inv.dataprovider.investment_group import InvestmentGroup
from gcm.inv.scenario import Scenario
from gcm.Dao.Utils.bulk_insert.sql_bulk_insert import SqlBulkInsert


class SingleNameEquityExposureInvestmentsGroupPersist(ReportingRunnerBase):
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

    @staticmethod
    def delete_rows(runner, as_of_date):
        def delete(dao, params):
            raw_sql = "DELETE FROM AnalyticsData.SingleNameEquityExposure WHERE AsOfDate = '{}'".format(as_of_date)
            conn = dao.data_engine.get_connection
            with conn.begin():
                conn.execute(raw_sql)

        runner.execute(params={}, source=DaoSource.InvestmentsDwh, operation=delete)

    def save_single_name_exposure(self):
        # short sector name mapping
        sector_short_names = dict(
            {'HEALTH CARE': 'Health Care', 'FINANCIALS': 'Financials', 'INFORMATION TECHNOLOGY': 'Info Tech',
             'CONSUMER DISCRETIONARY': 'Cons Discr', 'BROAD MARKET INDICES': 'Index', 'UTILITIES': 'Utilities',
             'OTHER': 'Other', 'INDUSTRIALS': 'Industrials', 'COMMUNICATION SERVICES': 'Comm Svcs', 'ENERGY': 'Energy',
             'REAL ESTATE': 'Real Estate', 'UTILITIES AND TELECOMMUNICATIONS': 'Utilities',
             'MATERIALS': 'Materials', 'CONSUMER STAPLES': 'Cons Staples', 'CONGLOMERATES': 'Other',
             'BIOTECHNOLOGY': 'Biotechnology', 'COMMUNICATIONS': 'Comm Svcs'})

        single_name = self._investment_group.get_single_name_exposure_by_investment_group(
            as_of_date=self._end_date,
        )
        inv_group_dimn = self._investment_group.get_dimensions()
        single_name_exposure = pd.merge(single_name, inv_group_dimn[['InvestmentGroupName', 'InvestmentGroupId']],
                                        how='inner', on=['InvestmentGroupName'])
        exposure_to_save = single_name_exposure[['InvestmentGroupId', 'Issuer', 'Sector', 'Cusip', 'AssetClass', 'ExpNav', 'AsOfDate']]
        remove_duplicated_pears = exposure_to_save[['Issuer', 'Sector']].drop_duplicates()
        dupliacted_Issuers = remove_duplicated_pears[remove_duplicated_pears[['Issuer']].duplicated()]['Issuer']
        exposure_to_save.loc[exposure_to_save['Sector'].isnull(), 'Sector'] = 'OTHER'
        exposure_to_save.loc[exposure_to_save['Sector'] == '', 'Sector'] = 'OTHER'
        exposure_to_save.loc[exposure_to_save['Issuer'].isin(dupliacted_Issuers.to_list()), 'Sector'] = 'OTHER'

        exposure_to_save = exposure_to_save.groupby(['InvestmentGroupId', 'Issuer', 'Sector', 'AssetClass', 'AsOfDate']).sum('ExpNav').reset_index()
        exposure_to_save['Sector'] = exposure_to_save['Sector'].map(sector_short_names)

        dwh_subscription = os.environ.get("Subscription", "nonprd")
        dwh_environment = os.environ.get("Environment", "dev").replace(
            "local", "dev"
        )
        config_params = {
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.InvestmentsDwh.name: {
                    "Environment": "dev",
                    "Subscription": "nonprd",
                },
                DaoSource.DataLake_Blob.name: {
                    "Environment": dwh_environment,
                    "Subscription": dwh_subscription,
                },
            }
        }

        runner = DaoRunner(
            container_lambda=lambda b, i: b.config.from_dict(i),
            config_params=config_params,
        )

        SingleNameEquityExposureInvestmentsGroupPersist.delete_rows(runner=runner, as_of_date=self._end_date)

        SqlBulkInsert().execute(
            runner=runner,
            df=exposure_to_save,
            target_source=DaoSource.InvestmentsDwh,
            target_schema='AnalyticsData',
            target_table='SingleNameEquityExposure',
            csv_params={"index_label": "Id", "float_format": "%.10f"},
            save=True,
        )

        return 'success'

    def run(self, **kwargs):
        return self.save_single_name_exposure()


if __name__ == "__main__":
    dwh_subscription = os.environ.get("Subscription", "nonprd")
    dwh_environment = os.environ.get("Environment", "dev").replace(
        "local", "dev"
    )
    runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.InvestmentsDwh.name: {
                    "Environment": dwh_environment,
                    "Subscription": dwh_subscription,
                },
                DaoSource.DataLake_Blob.name: {
                    "Environment": dwh_environment,
                    "Subscription": dwh_subscription,
                },
            }
        }
    )

    end_date = dt.date(2022, 12, 31)

    with Scenario(dao=runner, as_of_date=end_date).context():
        SingleNameEquityExposureInvestmentsGroupPersist().execute()
