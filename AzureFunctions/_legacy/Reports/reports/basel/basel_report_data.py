from gcm.inv.dataprovider.pub_dwh.allocation_query import (
    get_pub_dwh_portfolio_holdings,
)
from _legacy.core.reporting_runner_base import (
    ReportingRunnerBase,
)

# from gcm.inv.quantlib.timeseries.analytics import Analytics
import pandas as pd


class BaselReportData(ReportingRunnerBase):
    def __init__(self, runner, as_of_date, funds_exposure, portfolio):
        super().__init__(runner=runner)
        self._portfolio_name = portfolio
        self._as_of_date = as_of_date
        self._funds_exposure = funds_exposure
        self._runner = runner

    def portfolio_composition(self):
        allocation = get_pub_dwh_portfolio_holdings(self._runner, self._as_of_date, self._portfolio_name)

        return allocation

    def aggregated_exposure(self):
        fund_exposure = self._funds_exposure
        portfolio_allocation = self.portfolio_composition()
        fund_exposure = fund_exposure[fund_exposure["InvestmentName"].isin(portfolio_allocation["InvestmentName"].to_list())]
        portfolio_exp = pd.merge(
            fund_exposure,
            portfolio_allocation[["InvestmentName", "PctNav"]],
            how="left",
            on="InvestmentName",
        )
        # portfolio_exp_temp= portfolio_exp.iloc[2:]
        portfolio_exp_temp = portfolio_exp
        for c in portfolio_exp_temp.columns:
            if c not in ["InvestmentName", "LongShort", "metrics"]:
                portfolio_exp_temp[c] = portfolio_exp_temp[c].astype(float)

        result = pd.DataFrame()
        long_short_list = portfolio_exp_temp["LongShort"].unique()
        metrics_list = portfolio_exp_temp["metrics"].unique()
        for t1 in long_short_list:
            for t2 in metrics_list:
                temp = portfolio_exp_temp[(portfolio_exp_temp["LongShort"] == t1) & (portfolio_exp_temp["metrics"] == t2)]
                new_temp = temp.drop(columns=["InvestmentName", "LongShort", "metrics"])
                prodsum = new_temp.agg(lambda x: sum(x.mul(new_temp["PctNav"], axis=0, fill_value=0)))
                prodsum_df = prodsum.to_frame()
                result_df = pd.DataFrame(
                    {
                        "InvestmentName": [self._portfolio_name],
                        "LongShort": [t1],
                        "metrics": [t2],
                    }
                )
                result_df = pd.concat([result_df, prodsum_df.T], axis=1)
                result = result.append(result_df)
        result.drop(columns=["PctNav"], inplace=True)
        portfolio_exp.drop(columns=["PctNav"], inplace=True)
        portfolio_exp = portfolio_exp.append(result)

        return portfolio_exp

    def run(self, **kwargs):
        return self.aggregated_exposure()
