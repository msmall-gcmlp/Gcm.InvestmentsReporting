from datetime import datetime
from _legacy.Reports.reports.eof_risk_report.eof_external_report import (
    EofExternalData
)
from gcm.inv.scenario import Scenario
from gcm.Dao.DaoRunner import DaoRunner


def main(requestBody) -> str:
    params = requestBody["params"]
    run = params["run"]
    as_of_date = params["as_of_date"]
    as_of_date = datetime.strptime(as_of_date, "%Y-%m-%d").date()
    runner = DaoRunner()
    if run == "RunEofRiskReport":
        with Scenario(dao=runner, as_of_date=as_of_date).context():
            input_data = EofExternalData(
                runner=Scenario.get_attribute("dao"),
                as_of_date=as_of_date,
                scenario=["EOF External"]).execute()
        return input_data
    return None
