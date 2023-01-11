from datetime import datetime as dt
import pandas as pd
import math
from gcm.Dao.DaoSources import DaoSource

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


class EofLiquidityReport(ReportingRunnerBase):
    def __init__(
        self,
        runner,
        as_of_date,
        factor_inventory,
        manager_exposure
    ):
        super().__init__(runner=runner)
        self._as_of_date = as_of_date
        self._factor_inventory = factor_inventory
        self._manager_exposure = manager_exposure
        self._exposure = Exposure()

    def get_factor_stress_by_category(self, category):
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
        cum_shock_up = self._get_shocks_by_coverage(factor_inventory_by_category_up, coverage_ratio=0.5, sock_dn=False)
        cum_shock_dn = self._get_shocks_by_coverage(factor_inventory_by_category_dn, coverage_ratio=0.5, sock_dn=True)

        return cum_shock_dn, cum_shock_up

    def _get_shocks_by_coverage(self, factor_shocks, coverage_ratio=0.5, sock_dn=True):
        factor_shocks.sort_values(by='Directional_shock', ascending=sock_dn, inplace=True, ignore_index=True)
        factor_shocks['cum_shock'] = factor_shocks['Directional_shock'].cumsum()
        factor_shocks['pct_total'] = factor_shocks['cum_shock'] / factor_shocks['Directional_shock'].sum()
        # TODO:: get closest to the coverage ratio
        cum_shock = pd.DataFrame([factor_shocks[factor_shocks['pct_total'] <= coverage_ratio + 0.0001]['Directional_shock'].sum()], columns=['cum_shock'])
        top_factors = factor_shocks[factor_shocks['pct_total'] <= coverage_ratio + 0.0001][['GcmTicker', 'PortfolioExposure', 'Directional_shock']]
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
        total = pd.DataFrame(abs(style_factor_shock).values + abs(industry_factor_shock).values + abs(region_factor_shock).values
                             + beta + abs(idio).values, columns=['total_shock'])
        return total

    def generate_liquidity_stress_report(self):
        header_info = self.get_header_info()
        style_factor_shock_dn = self.get_factor_stress_by_category("Style_Factor")[0]['cum_shock']
        industry_factor_shock_dn = self.get_factor_stress_by_category("Industry_Factor")[0]['cum_shock']
        region_factor_shock_dn = self.get_factor_stress_by_category("Region_Factor")[0]['cum_shock']
        style_factor_shock_up = self.get_factor_stress_by_category("Style_Factor")[1]['cum_shock']
        industry_factor_shock_up = self.get_factor_stress_by_category("Industry_Factor")[1]['cum_shock']
        region_factor_shock_up = self.get_factor_stress_by_category("Region_Factor")[1]['cum_shock']
        top_regional_factors_dn = self.get_factor_stress_by_category("Region_Factor")[0]['top_factors']
        top_style_factors_dn = self.get_factor_stress_by_category("Style_Factor")[0]['top_factors']
        top_industry_factors_dn = self.get_factor_stress_by_category("Industry_Factor")[0]['top_factors']
        top_regional_factors_up = self.get_factor_stress_by_category("Region_Factor")[1]['top_factors']
        top_style_factors_up = self.get_factor_stress_by_category("Style_Factor")[1]['top_factors']
        top_industry_factors_up = self.get_factor_stress_by_category("Industry_Factor")[1]['top_factors']
        beta_shock_dn = self.get_beta_shock(shock=-0.16)
        idio_shock_dn = -1 * self.get_idio_shock(number_of_top_exposure=3, number_of_vol=2)['cum_shock']
        top_idio_shocks = self.get_idio_shock(number_of_top_exposure=3, number_of_vol=2)['top_exposures']
        beta_shock_up = self.get_beta_shock(shock=0.1)
        idio_shock_up = self.get_idio_shock(number_of_top_exposure=3, number_of_vol=2)['cum_shock']
        style_beta_dn = self.get_style_beta()[0]
        style_beta_up = self.get_style_beta()[1]
        total_dn = -1 * self.get_total_shock(style_factor_shock_dn,
                                             industry_factor_shock_dn,
                                             region_factor_shock_dn,
                                             beta_shock_dn,
                                             style_beta_dn,
                                             idio_shock_dn
                                             )
        total_up = self.get_total_shock(style_factor_shock_up,
                                        industry_factor_shock_up,
                                        region_factor_shock_up,
                                        beta_shock_up,
                                        style_beta_up,
                                        idio_shock_up
                                        )
        input_data = {
            "header_info": header_info,
            "Style_Shock_dn": style_factor_shock_dn,
            "Industry_Shock_dn": industry_factor_shock_dn,
            "Regional_Shock_dn": region_factor_shock_dn,
            "Style_Shock_up": style_factor_shock_up,
            "Industry_Shock_up": industry_factor_shock_up,
            "Regional_Shock_up": region_factor_shock_up,
            "Beta_Shock_dn": beta_shock_dn,
            "Idio_shock_dn": idio_shock_dn,
            "Beta_Shock_up": beta_shock_up,
            "Idio_shock_up": idio_shock_up,
            "Top_regional_up": top_regional_factors_up,
            "Top_style_up": top_style_factors_up,
            "Top_industry_up": top_industry_factors_up,
            "Top_regional_dn": top_regional_factors_dn,
            "Top_style_dn": top_style_factors_dn,
            "Top_industry_dn": top_industry_factors_dn,
            "Style_Beta_dn": style_beta_dn,
            "Style_Beta_up": style_beta_up,
            "Top_Idios": top_idio_shocks,
            "Total_dn": total_dn,
            "Total_up": total_up
        }

        as_of_date = dt.combine(self._as_of_date, dt.min.time())
        with Scenario(as_of_date=as_of_date).context():
            InvestmentsReportRunner().execute(
                data=input_data,
                template="EOF_Liquidity Shock_Template.xlsx",
                save=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.manager_fund_group,
                entity_name="Equity Opps Fund Ltd",
                entity_display_name="EOF",
                entity_ids=[19163],
                entity_source=DaoSource.PubDwh,
                report_name="Expected Liquidity Shortfall",
                report_type=ReportType.Risk,
                report_frequency="Daily",
                aggregate_intervals=AggregateInterval.Daily,
            )

    def run(self, **kwargs):
        self.generate_liquidity_stress_report()
        return True


if __name__ == "__main__":
    as_of_date = '2020-10-01'
    scenario = ["Liquidity Stress"],
    as_of_date = dt.strptime(as_of_date, "%Y-%m-%d").date()
    # config_params = {
    #     DaoRunnerConfigArgs.dao_global_envs.name: {
    #         # DaoSource.PubDwh.name: {
    #         #     "Environment": "prd",
    #         #     "Subscription": "prd",
    #         # },
    #         DaoSource.InvestmentsDwh.name: {
    #             "Environment": "prd",
    #             "Subscription": "prd",
    #         }
    #     }
    # }
    # runner = DaoRunner(
    #     container_lambda=lambda b, i: b.config.from_dict(i),
    #     config_params=config_params,
    # )
    with Scenario(as_of_date=as_of_date).context():
        input_data = EofStressTestingData(
            runner=Scenario.get_attribute("dao"),
            as_of_date=as_of_date,
            scenario=["Liquidity Stress"],
        ).execute()

        eof_liquidity = EofLiquidityReport(
            runner=Scenario.get_attribute("dao"),
            as_of_date=as_of_date,
            factor_inventory=input_data,
            manager_exposure=input_data,
        ).execute()
