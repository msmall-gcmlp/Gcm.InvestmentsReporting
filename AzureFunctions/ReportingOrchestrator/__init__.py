import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    requestBody: str = context.get_input()
    result = yield context.call_activity(
        "ReportingActivity", requestBody
    )
    return result


main = df.Orchestrator.create(orchestrator_function)
