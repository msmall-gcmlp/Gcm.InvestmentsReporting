from datetime import datetime
from gcm.inv.scenario import Scenario
from _legacy.Reports.reports.risk_model.single_name_exposure import SingleNameReport


def main(requestBody) -> str:
    params = requestBody["params"]
    run = params["run"]
    as_of_date = params["as_of_date"]
    as_of_date = datetime.strptime(as_of_date, "%Y-%m-%d").date()
    if run == 'RunSingleNameEquityExposure':
        with Scenario(as_of_date=as_of_date).context():
            single_name_equity_exposure = SingleNameReport()
            return single_name_equity_exposure.execute()
