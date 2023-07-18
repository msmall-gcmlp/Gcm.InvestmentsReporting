from datetime import datetime
from gcm.Dao.DaoRunner import DaoRunner
from _legacy.Reports.reports.performance_quality.xpfund_highlow_pq_screen import XPfundHighLowPQScreen


def main(requestBody) -> str:
    params = requestBody["params"]
    run = params["run"]
    as_of_date = params["as_of_date"]
    as_of_date = datetime.strptime(as_of_date, "%Y-%m-%d").date()

    if run == "XPFundHighLowActivity":
        return XPfundHighLowPQScreen(runner=DaoRunner(), as_of_date=as_of_date).execute()
