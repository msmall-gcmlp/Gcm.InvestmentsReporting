import azure.durable_functions as df
from ..legacy_tasks import LegacyTasks, ActivitySet, ActivityParams


def orchestrator_function(
    context: df.DurableOrchestrationContext,
) -> LegacyTasks:
    orchestrator_input: dict = context.get_input()
    return LegacyTasks(
        [
            ActivitySet(
                [ActivityParams("BbaReportActivity", orchestrator_input)]
            )
        ]
    )

    # yield context.call_activity(name='BbaReportActivity', input_=orchestrator_input)
