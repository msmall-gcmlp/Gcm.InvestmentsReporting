import os
from datetime import datetime as dt
import pandas as pd
import math
from gcm.Dao.DaoSources import DaoSource
from gcm.Dao.Utils.bulk_insert.sql_bulk_insert import SqlBulkInsert

from _legacy.Reports.reports.eof_liquidity_stress.report_data import EofStressTestingData
from _legacy.core.ReportStructure.report_structure import (
    ReportingEntityTypes,
    ReportType,
    AggregateInterval,
)
from _legacy.core.Runners.investmentsreporting import (
    InvestmentsReportRunner,
)
from gcm.inv.scenario import Scenario
from _legacy.core.reporting_runner_base import (
    ReportingRunnerBase,
)
from gcm.inv.dataprovider.attributes.exposure import Exposure
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs
from gcm.inv.dataprovider.entity_master import EntityMaster


class EofLiquidityReport(ReportingRunnerBase):
    def __init__(
        self,
        runner,
        as_of_date,
        factor_inventory,
        manager_exposure,
        correlated_factors,
    ):
        super().__init__(runner=runner)
        self._as_of_date = as_of_date
        self._factor_inventory = factor_inventory
        self._manager_exposure = manager_exposure
        self._exposure = Exposure()
        self._correlated_factors = correlated_factors
        self._runner = runner
        self._entity_master = EntityMaster()

    def get_inv_group_id(self):
        with Scenario(dao=self._runner).context():
            entity_master = self._entity_master.get_investment_entities(investment_names=['Equity Opps Fund Ltd'])
        inv_group_id = entity_master.InvestmentGroupId.unique()
        return inv_group_id

    @staticmethod
    def delete_rows(runner, as_of_date):
        def delete(dao, params):
            raw_sql = "DELETE FROM RiskModel.StressLoss WHERE AsOfDate = '{}'".format(as_of_date)
            conn = dao.data_engine.get_connection
            with conn.begin():
                conn.execute(raw_sql)

        runner.execute(params={}, source=DaoSource.InvestmentsDwh, operation=delete)

    def get_factor_stress_by_category(self, category, number_of_factors=3):
        factor_inventory = self._factor_inventory.copy()

        def _switch_factor_category(value):
            if 'EQUITY' in value:
                return 'Style_Factor'
            elif 'REGION' in value:
                return 'Region_Factor'
            elif 'INDUSTRY' in value:
                return 'Industry_Factor'

        factor_inventory.loc[factor_inventory["Type"] == "Region", "ParentFactor"] = 'REGION'

        factor_inventory = factor_inventory[~factor_inventory.ParentFactor.isnull()]
        # Beta is modeled in the beta shock, the Beta factor is excluded to avoid double counting
        factor_inventory = factor_inventory[factor_inventory['HierarchyParent'] != 'EQUITY_BETA_STYLE']
        factor_inventory['category'] = factor_inventory.apply(lambda x: _switch_factor_category(x['ParentFactor']), axis=1)
        factor_inventory_by_category = factor_inventory[factor_inventory['category'] == category]
        factor_inventory_by_category['Directional_shock'] = (factor_inventory['ShockSign'] *
                                                             factor_inventory['ShockMagnitude'] *
                                                             factor_inventory['PortfolioExposure']) / 100

        # takes all shocks that will drive portfolio up (ShockDirection ==Up for the short position and ShockDirection ==Dn for the long)
        factor_inventory_by_category_up = factor_inventory_by_category[
                                                                       (factor_inventory_by_category['ShockSign'] *
                                                                        factor_inventory_by_category['PortfolioExposure'] > 0)]
        factor_inventory_by_category_dn = factor_inventory_by_category[
                                                                       (factor_inventory_by_category['ShockSign'] *
                                                                        factor_inventory_by_category['PortfolioExposure'] <= 0)]
        # cum_shock_up = self._get_shocks_by_coverage(factor_inventory_by_category_up, coverage_ratio=0.5, sock_dn=False)
        # cum_shock_dn = self._get_shocks_by_coverage(factor_inventory_by_category_dn, coverage_ratio=0.5, sock_dn=True)

        cum_shock_up = self._get_shocks_by_factor_numbers(factor_inventory_by_category_up, category,
                                                          number_of_factors=number_of_factors, sock_dn=False)
        cum_shock_dn = self._get_shocks_by_factor_numbers(factor_inventory_by_category_dn, category,
                                                          number_of_factors=number_of_factors, sock_dn=True)

        return cum_shock_dn, cum_shock_up

    def _get_shocks_by_factor_numbers(self, factor_shocks, category, number_of_factors=3, sock_dn=True):
        multiplier = -1 if sock_dn else 1
        self._correlated_factors['Ticker'] = self._correlated_factors['Ticker'].str.upper()
        if (category == 'Style_Factor') or (category == 'Industry_Factor'):
            factor_shocks['GcmTicker'] = factor_shocks['GcmTicker'].str.split('_', n=1, expand=True)[1]
            correlated_factors = factor_shocks['GcmTicker'][factor_shocks['GcmTicker'].str.split('_', n=1, expand=True)[1].duplicated()].unique()
            # append if doesnt exist in the ticker
            correlated_factors = list(set(correlated_factors) - set(self._correlated_factors['Ticker'].to_list()))
            self._correlated_factors = pd.concat([self._correlated_factors, pd.DataFrame({'Reporting Name': correlated_factors,
                                                                                          'Ticker': correlated_factors})])
        if (category == 'Region_Factor'):
            factor_shocks['GcmTicker'] = factor_shocks['GcmTicker'].str.split('_', n=1, expand=True)[0]
            correlated_factors = factor_shocks['GcmTicker'][
                factor_shocks['GcmTicker'].str.split('_', n=1, expand=True)[0].duplicated()].unique()
            # append if doesnt exist in the ticker
            correlated_factors = list(set(correlated_factors) - set(self._correlated_factors['Ticker'].to_list()))
            self._correlated_factors = pd.concat([self._correlated_factors,
                                                  pd.DataFrame({'Reporting Name': correlated_factors,
                                                                'Ticker': correlated_factors})])

        factor_shocks = pd.merge(factor_shocks, self._correlated_factors, left_on='GcmTicker', right_on='Ticker', how='left')
        factor_shocks.loc[~factor_shocks['Ticker'].isnull(), 'GcmTicker'] = factor_shocks['Reporting Name']
        factor_shocks['exposure_sign'] = [1 if x >= 0.0 else -1 for x in factor_shocks['PortfolioExposure']]
        factor_shocks['Directional_shockv2'] = factor_shocks['Directional_shock'] * factor_shocks['exposure_sign']
        mask = ~factor_shocks['Ticker'].isnull()
        factor_shocks.loc[mask, 'Directional_shock'] = factor_shocks.loc[mask, 'Directional_shockv2']
        factor_exposure_info = factor_shocks
        factor_exposure_info['GcmTicker'] = factor_exposure_info['GcmTicker'].str.replace("_", " ")
        factor_shocks = factor_shocks[['Directional_shock', 'PortfolioExposure', 'GcmTicker']].groupby(['GcmTicker']).sum().reset_index()
        factor_shocks['Directional_shock'] = abs(factor_shocks['Directional_shock']) * multiplier

        factor_shocks.sort_values(by='Directional_shock', ascending=sock_dn, inplace=True, ignore_index=True)
        factor_shocks['cum_shock'] = factor_shocks['Directional_shock'].cumsum()
        factor_shocks['pct_total'] = factor_shocks['cum_shock'] / factor_shocks['Directional_shock'].sum()
        cum_shock = pd.DataFrame(
            [factor_shocks['Directional_shock'].head(number_of_factors).sum()],
            columns=['cum_shock'])
        top_factors = factor_shocks[
            ['GcmTicker', 'PortfolioExposure', 'Directional_shock']]
        top_factors['GcmTicker'] = top_factors['GcmTicker'].str.replace("_", " ")
        top_factors = top_factors[abs(top_factors['Directional_shock']) >= 0.001]
        output_to_save = factor_exposure_info[
            factor_exposure_info.GcmTicker.isin(top_factors.GcmTicker.head(number_of_factors))][['FactorShockTicker',
                                                                                                 'category',
                                                                                                 'Directional_shock',
                                                                                                 'GcmTicker']]
        return {'cum_shock': cum_shock, 'top_factors': top_factors.head(15), 'output_to_save': output_to_save}

    def _get_shocks_by_coverage(self, factor_shocks, coverage_ratio=0.5, sock_dn=True):
        factor_shocks.sort_values(by='Directional_shock', ascending=sock_dn, inplace=True, ignore_index=True)
        factor_shocks['cum_shock'] = factor_shocks['Directional_shock'].cumsum()
        factor_shocks['pct_total'] = factor_shocks['cum_shock'] / factor_shocks['Directional_shock'].sum()
        # TODO:: get closest to the coverage ratio
        cum_shock = pd.DataFrame([factor_shocks[factor_shocks['pct_total'] <= coverage_ratio + 0.0001]['Directional_shock'].sum()], columns=['cum_shock'])
        top_factors = factor_shocks[factor_shocks['pct_total'] <= coverage_ratio + 0.0001][['GcmTicker',
                                                                                            'PortfolioExposure',
                                                                                            'Directional_shock']]
        top_factors['GcmTicker'] = top_factors['GcmTicker'].str.replace("_", " ")
        return {'cum_shock': cum_shock, 'top_factors': top_factors.head(10)}

    def get_idio_shock(self, number_of_top_exposure=3, number_of_vol=2):
        exposure = self._exposure.get_eof_delta_adj_exp(self._as_of_date)
        exposure['pctIdio'] = exposure['pctIdio_ann'] / math.sqrt(12) * 0.01
        top_exposures = exposure['pctIdio'].head(number_of_top_exposure).sum()
        exposure = exposure[['SecurityName', 'DeltaAdjExp', 'SpecificResidualRisk', 'dollarIdio', 'exposure', 'pctIdio']]
        cum_shock = pd.DataFrame([top_exposures * number_of_vol], columns=['idio_shock'])
        return {'cum_shock': cum_shock, 'top_exposures': exposure.head(number_of_top_exposure)}

    def get_beta_shock(self, shock=0.2, beta=0.06):
        beta = self._exposure.get_eof_beta(self._as_of_date)
        return pd.DataFrame([shock * beta['beta'].values], columns=['beta_shock'])

    def get_style_beta(self):
        factor_inventory = self._factor_inventory.copy()
        # calculate shock for style beta seperately
        beta_shock = factor_inventory[factor_inventory['GcmTicker'] == 'US_BETA']

        beta_shock_up = beta_shock[
                                   (beta_shock['ShockSign'] *
                                    beta_shock['PortfolioExposure'] > 0)]
        beta_cum_shock_up = pd.DataFrame((beta_shock_up['ShockSign'] *
                                          beta_shock_up['ShockMagnitude'] *
                                          beta_shock_up['PortfolioExposure']) / 100, columns=['beta_shock_up'])

        beta_shock_dn = beta_shock[
                                   (beta_shock['ShockSign'] *
                                    beta_shock['PortfolioExposure'] <= 0)]
        beta_cum_shock_dn = pd.DataFrame((beta_shock_dn['ShockSign'] *
                                          beta_shock_dn['ShockMagnitude'] *
                                          beta_shock_dn['PortfolioExposure']) / 100, columns=['beta_shock_dn'])

        return beta_cum_shock_dn, beta_cum_shock_up

    def get_header_info(self):
        header = pd.DataFrame({"header_info": [self._as_of_date]})
        return header

    def get_total_shock(self, style_factor_shock, industry_factor_shock, region_factor_shock, beta_shock, style_beta, idio):
        beta = max(abs(beta_shock).values, abs(style_beta).values)
        total = pd.DataFrame(abs(style_factor_shock).values + abs(industry_factor_shock).values +
                             abs(region_factor_shock).values
                             + beta + abs(idio).values, columns=['total_shock'])
        return total

    def generate_liquidity_stress_report(self):
        style = 4
        region = 2
        industry = 14
        header_info = self.get_header_info()
        style_factor_shock_dn = self.get_factor_stress_by_category(category="Style_Factor", number_of_factors=style)[0][
            'cum_shock']
        industry_factor_shock_dn = self.get_factor_stress_by_category("Industry_Factor", number_of_factors=industry)[0][
            'cum_shock']
        region_factor_shock_dn = self.get_factor_stress_by_category("Region_Factor", number_of_factors=region)[0][
            'cum_shock']
        # style_factor_shock_up = self.get_factor_stress_by_category("Style_Factor", number_of_factors=style)[1]['cum_shock']
        # industry_factor_shock_up = self.get_factor_stress_by_category("Industry_Factor", number_of_factors=industry)[1]['cum_shock']
        # region_factor_shock_up = self.get_factor_stress_by_category("Region_Factor", number_of_factors=region)[1]['cum_shock']
        # output to save in the invdwh
        output_to_save_regional_factors_dn = self.get_factor_stress_by_category("Region_Factor", number_of_factors=region)[0][
            'output_to_save']
        output_to_save_style_factors_dn = self.get_factor_stress_by_category("Style_Factor", number_of_factors=style)[0][
            'output_to_save']
        output_to_save_industry_factors_dn = self.get_factor_stress_by_category("Industry_Factor", number_of_factors=industry)[0][
            'output_to_save']
        factor_output = pd.concat([output_to_save_regional_factors_dn, output_to_save_style_factors_dn, output_to_save_industry_factors_dn],
                            axis=0)
        top_regional_factors_dn = self.get_factor_stress_by_category("Region_Factor", number_of_factors=region)[0][
            'top_factors']
        top_style_factors_dn = self.get_factor_stress_by_category("Style_Factor", number_of_factors=style)[0][
            'top_factors']
        top_industry_factors_dn = self.get_factor_stress_by_category("Industry_Factor", number_of_factors=industry)[0][
            'top_factors']
        # top_regional_factors_up = self.get_factor_stress_by_category("Region_Factor", number_of_factors=region)[1]['top_factors']
        # top_style_factors_up = self.get_factor_stress_by_category("Style_Factor", number_of_factors=style)[1]['top_factors']
        # top_industry_factors_up = self.get_factor_stress_by_category("Industry_Factor", number_of_factors=industry)[1]['top_factors']
        beta_shock_dn = self.get_beta_shock(shock=-0.16)
        idio_shock_dn = -1 * self.get_idio_shock(number_of_top_exposure=3, number_of_vol=2)['cum_shock']
        top_idio_shocks = self.get_idio_shock(number_of_top_exposure=3, number_of_vol=2)['top_exposures']
        top_idio_shocks['pctIdio'] = -1 * top_idio_shocks['pctIdio']
        # beta_shock_up = self.get_beta_shock(shock=0.1)
        # idio_shock_up = self.get_idio_shock(number_of_top_exposure=3, number_of_vol=2)['cum_shock']
        style_beta_dn = self.get_style_beta()[0]
        min_beta = pd.DataFrame(min(style_beta_dn.values, beta_shock_dn.values), columns=['beta_shock'])
        # style_beta_up = self.get_style_beta()[1]
        total_dn = -1 * self.get_total_shock(style_factor_shock_dn,
                                             industry_factor_shock_dn,
                                             region_factor_shock_dn,
                                             beta_shock_dn,
                                             style_beta_dn,
                                             idio_shock_dn
                                             )
        idio_syst_output = pd.DataFrame.from_dict({'FactorShockTicker': ['Eq__Idio__Dn__Pct__Liquidity_Stress',
                                                                         'Eq__Market__Dn__Pct__Liquidity_Stress',
                                                                         'Eq__EOF__Dn__Pct__Liquidity_Stress'],
                                                   'category': ['Idio', 'Systematic', 'Total'],
                                                   'Directional_shock': [float(idio_shock_dn.values),
                                                                         float(min_beta.values),
                                                                         float(total_dn.values)]})
        output = pd.concat([factor_output, idio_syst_output])
        output['AsofDate'] = self._as_of_date
        output['category'] = output['category'].str.replace('_Factor', '')
        output.rename(columns={'category': 'FactorGroup', 'Directional_shock': 'StressLoss'}, inplace=True)
        output['InvestmentGroupId'] = int(self.get_inv_group_id())
        # total_up = self.get_total_shock(style_factor_shock_up,
        #                                 industry_factor_shock_up,
        #                                 region_factor_shock_up,
        #                                 beta_shock_up,
        #                                 style_beta_up,
        #                                 idio_shock_up)

        input_data = {
            "header_info": header_info,
            "Style_Shock_dn": style_factor_shock_dn,
            "Industry_Shock_dn": industry_factor_shock_dn,
            "Regional_Shock_dn": region_factor_shock_dn,
            "Idio_shock_dn": idio_shock_dn,
            "Top_regional_dn": top_regional_factors_dn.head(5),
            "Top_style_dn": top_style_factors_dn.head(15),
            "Top_industry_dn": top_industry_factors_dn[['PortfolioExposure', 'Directional_shock']].head(15),
            "Top_industry_dn_index": top_industry_factors_dn[['GcmTicker']].head(15),
            "Style_Beta_dn": min_beta,
            "Top_Idios": top_idio_shocks[['SpecificResidualRisk', 'exposure', 'pctIdio']],
            "Top_Idios_index": top_idio_shocks[['SecurityName']],
            "Total_dn": total_dn,
        }

        as_of_date = dt.combine(self._as_of_date, dt.min.time())
        with Scenario(as_of_date=as_of_date).context():
            InvestmentsReportRunner().execute(
                data=input_data,
                template="EOF_Liquidity Shock_Template.xlsx",
                save=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.manager_fund_group,
                entity_name="GCM Equity Opps Fund",
                entity_display_name="EOF",
                entity_ids=[640],
                entity_source=DaoSource.PubDwh,
                report_name="EOF Expected Liquidity Shortfall",
                report_type=ReportType.Risk,
                report_frequency="Daily",
                aggregate_intervals=AggregateInterval.Daily,
                output_dir="eof/"
            )

            EofLiquidityReport.delete_rows(runner=self._runner, as_of_date=self._as_of_date)
            factor_shockdimn = self._runner.execute(
                params={
                    "schema": "factors",
                    "table": "FactorShockDimn",
                },
                source=DaoSource.InvestmentsDwh,
                operation=lambda dao, params: dao.get_data(params),
            )
            missing_tickers = pd.DataFrame(
                list(set(output['FactorShockTicker'].to_list()) - set(factor_shockdimn['FactorShockTicker'].to_list())),
                columns=['FactorShockTicker'])

            SqlBulkInsert().execute(
                runner=self._runner,
                df=missing_tickers,
                target_source=DaoSource.InvestmentsDwh,
                target_schema='factors',
                target_table='FactorShockDimn',
                csv_params={"index_label": "FactorShockTickerId"},
                save=True,
            )
            SqlBulkInsert().execute(
                runner=self._runner,
                df=output,
                target_source=DaoSource.InvestmentsDwh,
                target_schema='RiskModel',
                target_table='StressLoss',
                csv_params={"index_label": "Id", "float_format": "%.10f"},
                save=True,
            )

    def run(self, **kwargs):
        self.generate_liquidity_stress_report()
        return True


if __name__ == "__main__":
    as_of_date = '2023-5-26'
    scenario = ["Liquidity Stress"],
    as_of_date = dt.strptime(as_of_date, "%Y-%m-%d").date()
    # persist results
    dwh_subscription = os.environ.get("Subscription", "prd")
    dwh_environment = os.environ.get("Environment", "prd").replace(
        "local", "dev"
    )
    config_params = {
        DaoRunnerConfigArgs.dao_global_envs.name: {
            DaoSource.PubDwh.name: {
                "Environment": "prd",
                "Subscription": "prd",
            },
            DaoSource.InvestmentsDwh.name: {
                "Environment": "prd",
                "Subscription": "prd",
            },
            DaoSource.ReportingStorage.name: {
                "Environment": "prd",
                "Subscription": "prd",
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
    with Scenario(dao=runner, as_of_date=as_of_date).context():
        input_data = EofStressTestingData(
            runner=Scenario.get_attribute("dao"),
            as_of_date=as_of_date,
            scenario=["Liquidity Stress"],
        ).execute()

        eof_liquidity = EofLiquidityReport(
            runner=Scenario.get_attribute("dao"),
            as_of_date=as_of_date,
            factor_inventory=input_data[0],
            manager_exposure=input_data[0],
            correlated_factors=input_data[1]

        ).execute()
