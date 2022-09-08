import pandas as pd
import datetime as dt
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.reporting.core.ReportStructure.report_structure import (
    ReportingEntityTypes,
    ReportType,
    ReportVertical,
)
from gcm.inv.reporting.core.Runners.investmentsreporting import (
    InvestmentsReportRunner,
)
from gcm.Scenario.scenario import Scenario
from .reporting_runner_base import ReportingRunnerBase


class BaselReport(ReportingRunnerBase):
    def __init__(
        self,
        runner,
        as_of_date,
        investment_name,
        mapping_to_template,
        input_data
    ):
        super().__init__(runner=runner)
        self._as_of_date = as_of_date
        self._input_data = input_data
        self._investment_name = investment_name
        self._mapping_to_template = mapping_to_template

    def get_header_info(self):
        header = pd.DataFrame(
            {"header_info": [self._investment_name]}
        )
        return header

    def get_as_of_date(self):
        header = pd.DataFrame(
            {"asofdate": [self._as_of_date]}
        )
        return header

    def get_exposure_by_category(self):
        data = self._input_data.copy()
        mapping = self._mapping_to_template
        for tab in mapping['Tab'].unique():
            if tab == 'Equity':
                column_equity = ['metrics'] + mapping[mapping['Tab'] == 'Equity']['TemplateMapping'].values.tolist()
                data_equity = data.loc[:, column_equity]
                data_equity = data_equity[data_equity['metrics'] == 'Market_Over_NAV']
                dic = data_equity.to_dict('list')
                for x, y in dic.items():
                    dic[x] = pd.DataFrame([y], columns=['Long', 'Short'])

            if tab == 'Derivative':
                columns_deriv = ['LongShort', 'metrics'] + mapping[mapping['Tab'] == 'Derivative']['TemplateMapping'].values.tolist()
                data_deriv = data.loc[:, columns_deriv]
                data_deriv_long = data_deriv[data_deriv['LongShort'] == 'Long'].sort_values(by='metrics', ascending=False)
                data_deriv_short = data_deriv[data_deriv['LongShort'] == 'Short'].sort_values(by='metrics', ascending=False)
                data_deriv_long = data_deriv_long.to_dict('list')
                data_deriv_short = data_deriv_short.to_dict('list')
                remove_keys = ['LongShort', 'metrics']
                [data_deriv_long.pop(x, None) for x in remove_keys]
                [data_deriv_short.pop(x, None) for x in remove_keys]
                # Create long/short sum
                for x in mapping[mapping['Tab'] == 'Derivative']['TemplateMapping'].values.tolist():
                    dic[x] = pd.DataFrame([data_deriv_long[x]], columns=['Notional', 'Market']) +\
                        pd.DataFrame([data_deriv_short[x]], columns=['Notional', 'Market'])

                data_deriv_long = {k + "_Long": v for k, v in data_deriv_long.items()}
                data_deriv_short = {k + "_Short": v for k, v in data_deriv_short.items()}
                data_deriv_long.update(data_deriv_short)
                for x, y in data_deriv_long.items():
                    dic[x] = pd.DataFrame([y], columns=['Notional', 'Market'])
            else:
                columns_repo = ['LongShort', 'metrics'] + mapping[mapping['Tab'].isin(['SFT (ReverseRepo)', 'SFT (Repo)'])]['TemplateMapping'].values.tolist()
                data_repo = data.loc[:, columns_repo]
                data_repo = data_repo[data_repo['metrics'] == 'Market_Over_NAV']
                data_repo_long = data_repo[data_repo['LongShort'] == 'Long'].sort_values(by='metrics', ascending=False)
                data_repo_short = data_repo[data_repo['LongShort'] == 'Short'].sort_values(by='metrics', ascending=False)
                data_repo_long = data_repo_long.to_dict('list')
                data_repo_short = data_repo_short.to_dict('list')
                remove_keys = ['LongShort', 'metrics']
                [data_repo_long.pop(x, None) for x in remove_keys]
                [data_repo_short.pop(x, None) for x in remove_keys]
                # Create long/short sum
                for x in mapping[mapping['Tab'].isin(['SFT (ReverseRepo)', 'SFT (Repo)'])]['TemplateMapping'].values.tolist():
                    dic[x] = pd.DataFrame([data_repo_long[x]], columns=['Market']) +\
                        pd.DataFrame([data_repo_short[x]], columns=['Market'])
        return dic

    def generate_basel_report(self):
        input_data = self.get_exposure_by_category()
        dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])
        # tuple(self._mapping_to_template['TemplateMapping'])
        wanted_keys = ('Equity_Public', 'Equity_Private',
                       'SovereignDebtotherthanJGBsA',
                       'SovereignDebtotherthanJGBsAAA',
                       'SovereignDebtotherthanJGBsBBB',
                       'SovereignDebtotherthanJGBsNotRated',
                       'SovereignDebtotherthanJGBsBB',
                       'SovereignDebtotherthanJGBsB',
                       'SovereignDebtotherthanJGBsBelowB',
                       'JGBsNotRated',
                       'FinancialInstitutionsDebAAA',
                       'FinancialInstitutionsDebA',
                       'FinancialInstitutionsDebBBB',
                       'FinancialInstitutionsDebBelowB',
                       'FinancialInstitutionsDebUnratedA',
                       'FinancialInstitutionsDebUnratedB',
                       'FinancialInstitutionsDebUnratedC',
                       'CorporateDebtincludinginsurancecompaniesAAA',
                       'CorporateDebtincludinginsurancecompaniesA',
                       'CorporateDebtincludinginsurancecompaniesBB',
                       'CorporateDebtincludinginsurancecompaniesBBB',
                       'CorporateDebtincludinginsurancecompaniesBelowBB',
                       'CorporateDebtincludinginsurancecompaniesNotRated',
                       'RealEstateDebt', 'SecuritizedResecuritizedAssetsAAA',
                       'SecuritizedResecuritizedAssetsAAPlus',
                       'SecuritizedResecuritizedAssetsAA',
                       'SecuritizedResecuritizedAssetsAAMinus',
                       'SecuritizedResecuritizedAssetsAPlus',
                       'SecuritizedResecuritizedAssetsA',
                       'SecuritizedResecuritizedAssetsAMinus',
                       'SecuritizedResecuritizedAssetsBBBPlus',
                       'SecuritizedResecuritizedAssetsBBB',
                       'SecuritizedResecuritizedAssetsBBBMinus',
                       'SecuritizedResecuritizedAssetsBBPlus',
                       'SecuritizedResecuritizedAssetsBB',
                       'SecuritizedResecuritizedAssetsBBMinus',
                       'SecuritizedResecuritizedAssetsBPlus',
                       'SecuritizedResecuritizedAssetsB',
                       'SecuritizedResecuritizedAssetsBMinus',
                       'SecuritizedResecuritizedAssetsBelowCCC',
                       'SecuritizedResecuritizedAssetsNotRated',
                       'FallBackApproach', 'SimpleApproach',
                       'InterestrateAAA',
                       'InterestrateBBB',
                       'ForeignexchangeAAA',
                       'ForeignexchangeA',
                       'ForeignexchangeBBB',
                       'CreditSingleNameBBB', 'CreditIndexIGBBB',
                       'CreditIndexSGBBB',
                       'CreditIndexSGUnratedA', 'CreditIndexSGUnratedB',
                       'CreditIndexSGUnratedC', 'EquitySingleNameAAA_Long',
                       'EquitySingleNameAAA_Short',
                       'EquitySingleNameBBB_Long', 'EquitySingleNameBBB_Short',
                       'EquityIndexAAA_Long', 'EquityIndexAAA_Short',
                       'CommodityOtherAAA', 'CommodityOtherBBB',
                       'RepoCollateralPaidSovBondAAA',
                       'RepoCollateralPaidSovBondA',
                       'RepoCollateralPaidSovBondBBB',
                       'RepoCollateralPaidSovBondBelowB',
                       'RepoCollateralPaidSovBondUnratedA',
                       'RepoCollateralPaidSovBondUnratedB',
                       'RepoCollateralPaidSovBondUnratedC')

        input_data = dictfilt(input_data, wanted_keys)
        input_data['header_info'] = self.get_header_info()
        input_data['asofdate'] = self.get_as_of_date()

        as_of_date = dt.datetime.combine(
            self._as_of_date, dt.datetime.min.time()
        )
        report_name = 'Basel_Report_' + self._investment_name + "_" +\
                      self._as_of_date.strftime("%Y%m%d")
        with Scenario(asofdate=as_of_date).context():
            InvestmentsReportRunner().execute(
                data=input_data,
                template="Basel Template.xlsx",
                save=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.cross_entity,
                entity_source=DaoSource.InvestmentsDwh,
                report_name=report_name,
                report_type=ReportType.Market,
                report_frequency="Daily",
                report_vertical=ReportVertical.FirmWide,
            )

    def run(self, **kwargs):
        self.generate_basel_report()
        return True
