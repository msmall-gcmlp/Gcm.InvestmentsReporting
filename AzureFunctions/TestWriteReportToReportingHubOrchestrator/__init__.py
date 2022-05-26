import azure.durable_functions as df

# http://localhost:7071/orchestrators/TestWriteReportToReportingHubOrchestrator


def orchestrator_function(context: df.DurableOrchestrationContext):
    yield context.call_activity(
        "TestWriteReportToReportingHubActivity"
    )
    return True


main = df.Orchestrator.create(orchestrator_function)
