import datetime as dt
import pandas as pd
import math
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.reporting.core.ReportStructure.report_structure import (
    ReportingEntityTypes,
    ReportType,
    AggregateInterval,
)
from gcm.inv.reporting.core.Runners.investmentsreporting import (
    InvestmentsReportRunner,
)
from gcm.inv.scenario import Scenario
from gcm.inv.reporting.core.reporting_runner_base import (
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
        factor_inventory_by_category['Abs_shock'] = abs(factor_inventory['ShockMagnitude'] * factor_inventory['PortfolioExposure'] / 100)

        # takes all shocks that will drive portfolio up (ShockDirection ==Up for the short position and ShockDirection ==Dn for the long)
        factor_inventory_by_category_up = factor_inventory_by_category[
                                                                       (factor_inventory_by_category['ShockSign'] *
                                                                       factor_inventory_by_category['PortfolioExposure'] > 0)]
        factor_inventory_by_category_dn = factor_inventory_by_category[
                                                                       (factor_inventory_by_category['ShockSign'] *
                                                                       factor_inventory_by_category['PortfolioExposure'] <= 0)]
        cum_shock_up = self._get_shocks_by_coverage(factor_inventory_by_category_up, coverage_ratio=0.5)
        cum_shock_dn = self._get_shocks_by_coverage(factor_inventory_by_category_dn, coverage_ratio=0.5)

        return cum_shock_dn, cum_shock_up

    def _get_shocks_by_coverage(self, factor_shocks, coverage_ratio=0.5):
        factor_shocks.sort_values(by='Abs_shock', ascending=False, inplace=True, ignore_index=True)
        factor_shocks['cum_shock'] = factor_shocks['Abs_shock'].cumsum()
        factor_shocks['pct_total'] = factor_shocks['cum_shock'] / factor_shocks['Abs_shock'].sum()
        # TODO:: get closest to the coverage ratio
        cum_shock = pd.DataFrame([factor_shocks[factor_shocks['pct_total'] <= coverage_ratio + 0.0001]['Abs_shock'].sum()], columns=['cum_shock'])
        top_factors = factor_shocks[factor_shocks['pct_total'] <= coverage_ratio + 0.0001][['GcmTicker', 'PortfolioExposure', 'Abs_shock']]
        top_factors['GcmTicker'] = top_factors['GcmTicker'].str.replace("_", " ")
        return {'cum_shock': cum_shock, 'top_factors': top_factors.head(10)}

    def get_idio_shock(self, number_of_top_exposure=3, number_of_vol=2):
        exposure = self._exposure.get_eof_delta_adj_exp(self._as_of_date)
        exposure['pctIdio'] = exposure['pctIdio_ann'] / math.sqrt(12)
        top_exposures = exposure['pctIdio'].head(number_of_top_exposure).sum() * 0.01
        return pd.DataFrame([top_exposures * number_of_vol], columns=['idio_shock'])

    def get_beta_shock(self, shock=0.2, beta=0.06):
        beta = self._exposure.get_eof_beta(self._as_of_date)
        return pd.DataFrame([shock * beta['beta'].values], columns=['beta_shock'])

    def get_header_info(self):
        header = pd.DataFrame({"header_info": [self._as_of_date]})
        return header

    def generate_liquidity_stress_report(self):
        header_info = self.get_header_info()
        style_factor_shock_dn = -1 * self.get_factor_stress_by_category("Style_Factor")[0]['cum_shock']
        industry_factor_shock_dn = -1 * self.get_factor_stress_by_category("Industry_Factor")[0]['cum_shock']
        region_factor_shock_dn = -1 * self.get_factor_stress_by_category("Region_Factor")[0]['cum_shock']
        style_factor_shock_up = self.get_factor_stress_by_category("Style_Factor")[1]['cum_shock']
        industry_factor_shock_up = self.get_factor_stress_by_category("Industry_Factor")[1]['cum_shock']
        region_factor_shock_up = self.get_factor_stress_by_category("Region_Factor")[1]['cum_shock']
        top_regional_factors_dn = self.get_factor_stress_by_category("Region_Factor")[0]['top_factors']
        top_style_factors_dn = self.get_factor_stress_by_category("Style_Factor")[0]['top_factors']
        top_industry_factors_dn = self.get_factor_stress_by_category("Industry_Factor")[0]['top_factors']
        top_regional_factors_up = self.get_factor_stress_by_category("Region_Factor")[1]['top_factors']
        top_style_factors_up = self.get_factor_stress_by_category("Style_Factor")[1]['top_factors']
        top_industry_factors_up = self.get_factor_stress_by_category("Industry_Factor")[1]['top_factors']
        beta_shock_dn = -1 * self.get_beta_shock(shock=0.2, beta=0.06)
        idio_shock_dn = -1 * self.get_idio_shock(number_of_top_exposure=3, number_of_vol=2)
        beta_shock_up = self.get_beta_shock(shock=0.2, beta=0.06)
        idio_shock_up = self.get_idio_shock(number_of_top_exposure=3, number_of_vol=2)
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
        }

        as_of_date = dt.datetime.combine(self._as_of_date, dt.datetime.min.time())
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
                report_name= "EOF Liquidity Stress",
                report_type=ReportType.Risk,
                report_frequency="Daily",
                aggregate_intervals=AggregateInterval.Daily,
            )

    def run(self, **kwargs):
        self.generate_liquidity_stress_report()
        return True
