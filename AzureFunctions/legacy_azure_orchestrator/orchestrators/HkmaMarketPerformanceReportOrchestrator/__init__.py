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
