import azure.durable_functions as df
from ..legacy_tasks import LegacyTasks, ActivitySet, ActivityParams


def orchestrator_function(context: df.DurableOrchestrationContext) -> LegacyTasks:
    client_input: dict = context.get_input()
    params = client_input["params"]
    params.update({"run": "HkmaMarketPerformanceReport"})
    params = {"params": params, "data": {}}

    # yield context.call_activity(
    #     "HkmaMarketPerformanceReportActivity",
    #     params,
    # )

    # return True
    return LegacyTasks(
        [
            ActivitySet(
                [
                    ActivityParams(
                        "HkmaMarketPerformanceReportActivity", params
                    )
                ]
            )
        ]
    )


# main = df.Orchestrator.create(orchestrator_function)
