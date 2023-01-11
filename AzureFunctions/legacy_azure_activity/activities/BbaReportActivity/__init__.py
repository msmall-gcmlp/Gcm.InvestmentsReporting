from datetime import datetime
from gcm.inv.scenario import Scenario
from _legacy.Reports.reports.brinson_based_attribution.bba_report import (
    BbaReport,
)


def main(requestBody) -> str:
    params = requestBody["params"]
    acronyms = params.get("acronym", None)
    if acronyms is not None:
        acronyms = acronyms.split(",")

    as_of_date = params["as_of_date"]
    firm_only = params.get("firm_only", 0)

    as_of_date = datetime.strptime(as_of_date, "%Y-%m-%d").date()

    with Scenario(as_of_date=as_of_date).context():
        bba_report = BbaReport()
        return bba_report.execute(acronyms=acronyms, firm_only=firm_only)
