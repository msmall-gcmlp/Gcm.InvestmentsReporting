from datetime import datetime
from gcm.Scenario.scenario import Scenario
from gcm.Dao.DaoRunner import DaoRunner
from gcm.inv.quantlib.enum_source import PeriodicROR

from Reports.reports.brinson_based_attribution.bba_report import BbaReport
from Reports.reports.eof_rba_report import EofReturnBasedAttributionReport

def main(requestBody) -> str:
    params = requestBody["params"]
    acronyms = params.get("acronym", None)
    if acronyms is not None:
        acronyms = acronyms.split(",")

    asofdate = params["asofdate"]
    firm_only = params.get("firm_only", 0)

    as_of_date = datetime.strptime(asofdate, "%Y-%m-%d").date()
    runner = DaoRunner()

    with Scenario(runner=runner, as_of_date=as_of_date).context():
        bba_report = BbaReport()
        return bba_report.execute(acronyms=acronyms, firm_only=firm_only)
