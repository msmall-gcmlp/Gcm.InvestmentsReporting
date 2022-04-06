import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    requestBody: str = context.get_input()
    param = "coolParameter"
    coolParameter = ("This is a very cool, but default parameter"
                     if requestBody is None or param not in requestBody
                     else requestBody[param])

    result = yield context.call_activity('Activity1', coolParameter)
    return result


main = df.Orchestrator.create(orchestrator_function)
