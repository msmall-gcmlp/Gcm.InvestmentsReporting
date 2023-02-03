from datetime import datetime

from gcm.Dao.DaoRunner import DaoRunner
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.scenario import Scenario

from _legacy.Reports.reports.risk_model.single_name_exposure_investmentgroup import SingleNameInvestmentGroupReport
from _legacy.Reports.reports.risk_model.single_name_exposure_portfolio import SingleNamePortfolioReport
from _legacy.Reports.reports.risk_model.single_name_exposure_investments_group_persist import \
    SingleNameEquityExposureInvestmentsGroupPersist
from _legacy.Reports.reports.risk_model.single_name_exposure_summary import SingleNameEquityExposureSummary


def main(requestBody) -> str:
    params = requestBody["params"]
    run = params["run"]
    as_of_date = params["as_of_date"]
    as_of_date = datetime.strptime(as_of_date, "%Y-%m-%d").date()
    config_params = {
        DaoSource.InvestmentsDwh.name: {
            "Environment": "dev",
            "Subscription": "nonprd",
        }
    }
    runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params=config_params,
    )
    if run == 'RunSingleNameEquityExposureInvestmentGroupPersist':
        with Scenario(dao=runner, as_of_date=as_of_date).context():
            return SingleNameEquityExposureInvestmentsGroupPersist().execute()
    elif run == 'RunSingleNameEquityExposureInvestmentGroupReport':
        with Scenario(dao=runner, as_of_date=as_of_date).context():
            return SingleNameInvestmentGroupReport().execute()
    elif run == 'RunSingleNameEquityExposurePortfolio':
        with Scenario(dao=runner, as_of_date=as_of_date).context():
            single_name_equity_exposure = SingleNamePortfolioReport()
            return single_name_equity_exposure.execute()
    elif run == 'RunSingleNameEquityExposureSummary':
        with Scenario(dao=runner, as_of_date=as_of_date).context():
            return SingleNameEquityExposureSummary().execute()
