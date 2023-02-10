from datetime import datetime
from gcm.Dao.DaoRunner import DaoRunner

from _legacy.Reports.reports.eof_crowding.run_eof_crowding_report import RunEofCrowdingReport


def main(requestBody) -> str:
    params = requestBody["params"]
    run = params["run"]
    as_of_date = params["as_of_date"]
    as_of_date = datetime.strptime(as_of_date, "%Y-%m-%d").date()

    if run == "EofCrowdingReportActivity":
        return RunEofCrowdingReport(runner=DaoRunner(), as_of_date=as_of_date).execute()
