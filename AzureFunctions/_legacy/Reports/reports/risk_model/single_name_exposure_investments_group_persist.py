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

    def save_single_name_exposure(self):
        single_name = self._investment_group.overlay_singlename_exposure(
            start_date=self._start_date,
            end_date=self._end_date,
        )

        inv_group_dimn = self._investment_group.get_dimensions()
        single_name_exposure = pd.merge(single_name, inv_group_dimn[['InvestmentGroupName', 'InvestmentGroupId']],
                                        how='inner', on=['InvestmentGroupName'])
        exposure_to_save = single_name_exposure[['InvestmentGroupId', 'Issuer', 'Sector', 'Cusip', 'ExpNav', 'AsOfDate']]
        dupliacted_Issuers = exposure_to_save[exposure_to_save[['Issuer']].duplicated()]['Issuer']
        exposure_to_save.loc[exposure_to_save['Issuer'].isin(dupliacted_Issuers.to_list()), 'Sector'] = 'Other'

        dwh_subscription = os.environ.get("Subscription", "nonprd")
        dwh_environment = os.environ.get("Environment", "dev").replace(
            "local", "dev"
        )
        config_params = {
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

        runner = DaoRunner(
            container_lambda=lambda b, i: b.config.from_dict(i),
            config_params=config_params,
        )
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

    end_date = dt.date(2022, 9, 30)

    with Scenario(dao=runner, as_of_date=end_date).context():
        SingleNameEquityExposureInvestmentsGroupPersist().execute()
