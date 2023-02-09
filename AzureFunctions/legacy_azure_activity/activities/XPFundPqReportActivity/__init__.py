from datetime import datetime
from gcm.Dao.DaoRunner import DaoRunner
from _legacy.Reports.reports.performance_quality.run_xpfund_pq_report import RunXPFundPqReport


def main(requestBody) -> str:
    params = requestBody["params"]
    run = params["run"]
    as_of_date = params["as_of_date"]
    as_of_date = datetime.strptime(as_of_date, "%Y-%m-%d").date()

    if run == "XPFundPqReportActivity":
        return RunXPFundPqReport(runner=DaoRunner(), as_of_date=as_of_date).execute()
