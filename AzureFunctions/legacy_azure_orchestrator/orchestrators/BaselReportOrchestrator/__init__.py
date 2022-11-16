import azure.durable_functions as df
from ..legacy_tasks import LegacyTasks, ActivitySet, ActivityParams
from ...legacy_report_orch_parsed_args import LegacyReportingOrchParsedArgs


def orchestrator_function(
    context: df.DurableOrchestrationContext,
) -> LegacyTasks:
    # get factor Returns
    client_input = LegacyReportingOrchParsedArgs.parse_client_inputs(
        context
    )
    params = client_input["params"]
    disct1 = {
        "params": {
            "run": "RunBaselReport",
            "as_of_date": params["as_of_date"],
            "balancedate": params["balancedate"],
            "portfolio_name": params["portfolio_name"],
        },
    }
    return LegacyTasks(
        [ActivitySet([ActivityParams("BaselReportActivity", disct1)])]
    )
