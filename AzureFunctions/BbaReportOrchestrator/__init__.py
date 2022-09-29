import azure.durable_functions as df

def orchestrator_function(context: df.DurableOrchestrationContext):
    orchestrator_input: dict = context.get_input()
    yield context.call_activity(name='BbaReportActivity', input_=orchestrator_input)


main = df.Orchestrator.create(orchestrator_function)
