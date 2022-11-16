import azure.durable_functions as df
from ..legacy_tasks import LegacyTasks, ActivitySet, ActivityParams


def orchestrator_function(
    context: df.DurableOrchestrationContext,
) -> LegacyTasks:
    # get factor Returns
    client_input: dict = context.get_input()
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
