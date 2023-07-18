from datetime import datetime
from gcm.Dao.DaoRunner import DaoRunner
from _legacy.Reports.reports.performance_quality.run_xpfund_pq_report import RunXPFundPqReport
from _legacy.Reports.reports.performance_quality.xpfund_highlow_pq_screen import XPfundHighLowPQScreen


def main(requestBody) -> str:
    params = requestBody["params"]
    run = params["run"]
    as_of_date = params["as_of_date"]
    as_of_date = datetime.strptime(as_of_date, "%Y-%m-%d").date()

    if run == "XPFundPqReportActivity":
        run_firmwide_xpfund = RunXPFundPqReport(runner=DaoRunner(), as_of_date=as_of_date).execute()
        run_highlow = XPfundHighLowPQScreen(runner=DaoRunner(), as_of_date=as_of_date).execute()
        return run_firmwide_xpfund, run_highlow
