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
    activity_params = []
    for periodicity in ["ITD", "YTD"]:
        params.update({"run": "EofRbaReport"})
        params.update({"periodicity": periodicity})
        period_params = {"params": params, "data": {}}
        activity_params.append(
            ActivityParams("EofRbaReportActivity", period_params)
        )
    return LegacyTasks(ActivitySet(activity_params=activity_params))
