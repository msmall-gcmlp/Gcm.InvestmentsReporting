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
            "run": "RunEofLiquidityStress",
            "as_of_date": params["as_of_date"],
        },
    }
    return LegacyTasks(
        [
            ActivitySet(
                [
                    ActivityParams(
                        "EofLiquidityStressReportActivity", disct1
                    )
                ]
            )
        ]
    )
