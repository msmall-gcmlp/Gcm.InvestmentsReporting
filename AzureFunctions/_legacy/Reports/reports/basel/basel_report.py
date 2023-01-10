import pandas as pd
from datetime import datetime as dt
from gcm.Dao.DaoRunner import DaoRunnerConfigArgs, DaoRunner
from gcm.Dao.DaoSources import DaoSource
from _legacy.core.ReportStructure.report_structure import (
    ReportingEntityTypes,
    ReportType,
    ReportVertical,
)
from _legacy.core.Runners.investmentsreporting import (
    InvestmentsReportRunner,
)
from gcm.inv.scenario import Scenario
from _legacy.core.reporting_runner_base import (
    ReportingRunnerBase,
)
from gcm.Dao.Utils.tabular_data_util_outputs import TabularDataOutputTypes
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from gcm.Dao.daos.azure_datalake.azure_datalake_file import AzureDataLakeFile

from _legacy.Reports.reports.basel.basel_report_data import BaselReportData


class BaselReport(ReportingRunnerBase):
    def __init__(
        self,
        runner,
        as_of_date,
        investment_name,
        mapping_to_template,
        input_data,
    ):
        super().__init__(runner=runner)
        self._as_of_date = as_of_date
        self._input_data = input_data
        self._investment_name = investment_name
        self._mapping_to_template = mapping_to_template

    def get_header_info(self):
        header = pd.DataFrame({"header_info": [self._investment_name]})
        return header

    def get_as_of_date(self):
        header = pd.DataFrame({"as_of_date": [self._as_of_date]})
        return header

    def get_exposure_by_category(self):
        data = self._input_data.copy()
        mapping = self._mapping_to_template
        for tab in mapping["Tab"].unique():
            if tab == "Equity":
                column_equity = ["metrics"] + mapping[mapping["Tab"] == "Equity"]["TemplateMapping"].values.tolist()
                data_equity = data.loc[:, column_equity]
                data_equity = data_equity[data_equity["metrics"] == "Market_Over_NAV"]
                dic = data_equity.to_dict("list")
                for x, y in dic.items():
                    dic[x] = pd.DataFrame([y], columns=["Long", "Short"])

            if tab == "Derivative":
                columns_deriv = ["LongShort", "metrics"] + mapping[mapping["Tab"] == "Derivative"]["TemplateMapping"].values.tolist()
                data_deriv = data.loc[:, columns_deriv]
                data_deriv_long = data_deriv[data_deriv["LongShort"] == "Long"].sort_values(by="metrics", ascending=False)
                data_deriv_short = data_deriv[data_deriv["LongShort"] == "Short"].sort_values(by="metrics", ascending=False)
                data_deriv_long = data_deriv_long.to_dict("list")
                data_deriv_short = data_deriv_short.to_dict("list")
                remove_keys = ["LongShort", "metrics"]
                [data_deriv_long.pop(x, None) for x in remove_keys]
                [data_deriv_short.pop(x, None) for x in remove_keys]
                # Create long/short sum
                for x in mapping[mapping["Tab"] == "Derivative"]["TemplateMapping"].values.tolist():
                    dic[x] = pd.DataFrame([data_deriv_long[x]], columns=["Notional", "Market"],) + pd.DataFrame(
                        [data_deriv_short[x]],
                        columns=["Notional", "Market"],
                    )

                data_deriv_long = {k + "_Long": v for k, v in data_deriv_long.items()}
                data_deriv_short = {k + "_Short": v for k, v in data_deriv_short.items()}
                data_deriv_long.update(data_deriv_short)
                for x, y in data_deriv_long.items():
                    dic[x] = pd.DataFrame([y], columns=["Notional", "Market"])
            else:
                columns_repo = ["LongShort", "metrics"] + mapping[mapping["Tab"].isin(["SFT (ReverseRepo)", "SFT (Repo)"])]["TemplateMapping"].values.tolist()
                data_repo = data.loc[:, columns_repo]
                data_repo = data_repo[data_repo["metrics"] == "Market_Over_NAV"]
                data_repo_long = data_repo[data_repo["LongShort"] == "Long"].sort_values(by="metrics", ascending=False)
                data_repo_short = data_repo[data_repo["LongShort"] == "Short"].sort_values(by="metrics", ascending=False)
                data_repo_long = data_repo_long.to_dict("list")
                data_repo_short = data_repo_short.to_dict("list")
                remove_keys = ["LongShort", "metrics"]
                [data_repo_long.pop(x, None) for x in remove_keys]
                [data_repo_short.pop(x, None) for x in remove_keys]
                # Create long/short sum
                for x in mapping[mapping["Tab"].isin(["SFT (ReverseRepo)", "SFT (Repo)"])]["TemplateMapping"].values.tolist():
                    dic[x] = pd.DataFrame([data_repo_long[x]], columns=["Market"]) + pd.DataFrame([data_repo_short[x]], columns=["Market"])
        return dic

    def generate_basel_report(self):
        input_data = self.get_exposure_by_category()
        dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])
        # tuple(self._mapping_to_template['TemplateMapping'])
        wanted_keys = (
            "Equity_Public",
            "Equity_Private",
            "SovereignDebtotherthanJGBsA",
            "SovereignDebtotherthanJGBsAAA",
            "SovereignDebtotherthanJGBsBBB",
            "SovereignDebtotherthanJGBsNotRated",
            "SovereignDebtotherthanJGBsBB",
            "SovereignDebtotherthanJGBsB",
            "SovereignDebtotherthanJGBsBelowB",
            "JGBsNotRated",
            "FinancialInstitutionsDebAAA",
            "FinancialInstitutionsDebA",
            "FinancialInstitutionsDebBBB",
            "FinancialInstitutionsDebBelowB",
            "FinancialInstitutionsDebUnratedA",
            "FinancialInstitutionsDebUnratedB",
            "FinancialInstitutionsDebUnratedC",
            "CorporateDebtincludinginsurancecompaniesAAA",
            "CorporateDebtincludinginsurancecompaniesA",
            "CorporateDebtincludinginsurancecompaniesBB",
            "CorporateDebtincludinginsurancecompaniesBBB",
            "CorporateDebtincludinginsurancecompaniesBelowBB",
            "CorporateDebtincludinginsurancecompaniesNotRated",
            "RealEstateDebt",
            "SecuritizedResecuritizedAssetsAAA",
            "SecuritizedResecuritizedAssetsAAPlus",
            "SecuritizedResecuritizedAssetsAA",
            "SecuritizedResecuritizedAssetsAAMinus",
            "SecuritizedResecuritizedAssetsAPlus",
            "SecuritizedResecuritizedAssetsA",
            "SecuritizedResecuritizedAssetsAMinus",
            "SecuritizedResecuritizedAssetsBBBPlus",
            "SecuritizedResecuritizedAssetsBBB",
            "SecuritizedResecuritizedAssetsBBBMinus",
            "SecuritizedResecuritizedAssetsBBPlus",
            "SecuritizedResecuritizedAssetsBB",
            "SecuritizedResecuritizedAssetsBBMinus",
            "SecuritizedResecuritizedAssetsBPlus",
            "SecuritizedResecuritizedAssetsB",
            "SecuritizedResecuritizedAssetsBMinus",
            "SecuritizedResecuritizedAssetsBelowCCC",
            "SecuritizedResecuritizedAssetsNotRated",
            "FallBackApproach",
            "SimpleApproach",
            "InterestrateAAA",
            "InterestrateBBB",
            "ForeignexchangeAAA",
            "ForeignexchangeA",
            "ForeignexchangeBBB",
            "CreditSingleNameBBB",
            "CreditIndexIGBBB",
            "CreditIndexSGBBB",
            "CreditIndexSGUnratedA",
            "CreditIndexSGUnratedB",
            "CreditIndexSGUnratedC",
            "EquitySingleNameAAA_Long",
            "EquitySingleNameAAA_Short",
            "EquitySingleNameBBB_Long",
            "EquitySingleNameBBB_Short",
            "EquityIndexAAA_Long",
            "EquityIndexAAA_Short",
            "CommodityOtherAAA",
            "CommodityOtherBBB",
            "RepoCollateralPaidSovBondAAA",
            "RepoCollateralPaidSovBondA",
            "RepoCollateralPaidSovBondBBB",
            "RepoCollateralPaidSovBondBelowB",
            "RepoCollateralPaidSovBondUnratedA",
            "RepoCollateralPaidSovBondUnratedB",
            "RepoCollateralPaidSovBondUnratedC",
        )

        input_data = dictfilt(input_data, wanted_keys)
        input_data["header_info"] = self.get_header_info()
        input_data["asofdate"] = self.get_as_of_date()

        as_of_date = dt.combine(self._as_of_date, dt.min.time())
        report_name = "Basel_Report_" + self._investment_name + "_" + self._as_of_date.strftime("%Y%m%d")
        with Scenario(runner=DaoRunner(), as_of_date=as_of_date).context():
            InvestmentsReportRunner().execute(
                data=input_data,
                template="Basel Template.xlsx",
                save=True,
                save_as_pdf=False,
                runner=self._runner,
                entity_type=ReportingEntityTypes.cross_entity,
                entity_source=DaoSource.InvestmentsDwh,
                report_name=report_name,
                report_type=ReportType.Market,
                report_frequency="Daily",
                report_vertical=ReportVertical.FIRM,
            )

    def run(self, **kwargs):
        self.generate_basel_report()
        return True


if __name__ == "__main__":
    as_of_date = '2022-09-30'
    balancedate = '2022-10-31'
    portfolio_names = ['YAKUMO', 'AMATSU ']
    as_of_date = dt.strptime(as_of_date, "%Y-%m-%d").date()
    balancedate = dt.strptime(balancedate, "%Y-%m-%d").date()
    config_params = {
        DaoRunnerConfigArgs.dao_global_envs.name: {
            DaoSource.PubDwh.name: {
                "Environment": "prd",
                "Subscription": "prd",
            },
            DaoSource.InvestmentsDwh.name: {
                "Environment": "prd",
                "Subscription": "prd",
            }
        }
    }
    runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params=config_params,
    )

    file_name = "truview_output.csv"
    mapping_file_name = "basel_mapping.csv"
    folder = "basel"
    loc = "raw/investmentsreporting/underlyingdata/"
    location = f"{loc}/{folder}/"
    params = AzureDataLakeDao.create_get_data_params(location, file_name, True)
    params_mapping = AzureDataLakeDao.create_get_data_params(location, mapping_file_name, True)
    file: AzureDataLakeFile = runner.execute(
        params=params,
        source=DaoSource.DataLake,
        operation=lambda dao, params: dao.get_data(params),
    )
    df = file.to_tabular_data(TabularDataOutputTypes.PandasDataFrame, params)

    mapping_file: AzureDataLakeFile = runner.execute(
        params=params_mapping,
        source=DaoSource.DataLake,
        operation=lambda dao, params_mapping: dao.get_data(params_mapping),
    )
    df_mapping = mapping_file.to_tabular_data(TabularDataOutputTypes.PandasDataFrame, params_mapping)
    for portfolio_name in portfolio_names:
        runner2 = DaoRunner(
            container_lambda=lambda b, i: b.config.from_dict(i),
            config_params=config_params,
        )
        portfolio_allocation = BaselReportData(
            runner=runner2,
            as_of_date=balancedate,
            funds_exposure=df,
            portfolio=portfolio_name,
        ).execute()
        for investment_name in portfolio_allocation["InvestmentName"].unique():
            data_per_investment = portfolio_allocation[portfolio_allocation["InvestmentName"] == investment_name]
            BaselReport(
                runner=runner2,
                as_of_date=as_of_date,
                investment_name=investment_name,
                input_data=data_per_investment,
                mapping_to_template=df_mapping,
            ).execute()

