from datetime import datetime
from gcm.inv.scenario import Scenario
from _legacy.Reports.reports.portfolio_construction.run_portfolio_construction_report import PortfolioConstructionReport


def main(requestBody) -> str:
    params = requestBody["params"]
    run = params["run"]
    as_of_date = params["as_of_date"]
    acronym = params["acronym"]
    scenario_name = params["scenario_name"]
    as_of_date = datetime.strptime(as_of_date, "%Y-%m-%d").date()

    if run == "PortfolioConstructionReport":
        with Scenario(as_of_date=as_of_date).context():
            report = PortfolioConstructionReport()
            return report.execute(portfolio_acronym=acronym,
                                  scenario_name=scenario_name,
                                  as_of_date=as_of_date)
