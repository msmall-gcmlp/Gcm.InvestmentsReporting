import azure.durable_functions as df
from ..legacy_tasks import LegacyTasks, ActivitySet, ActivityParams
from ...legacy_report_orch_parsed_args import LegacyReportingOrchParsedArgs


def orchestrator_function(
    context: df.DurableOrchestrationContext,
) -> LegacyTasks:
    orchestrator_input: dict = (
        LegacyReportingOrchParsedArgs.parse_client_inputs(context)
    )
    return LegacyTasks(
        [
            ActivitySet(
                [ActivityParams("BbaReportActivity", orchestrator_input)]
            )
        ]
    )

    # yield context.call_activity(name='BbaReportActivity', input_=orchestrator_input)
