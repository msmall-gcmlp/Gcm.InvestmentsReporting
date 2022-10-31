import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    client_input: dict = context.get_input()
    params = client_input["params"]
    params.update({"run": "HkmaMarketPerformanceReport"})
    params = {"params": params, "data": {}}

    yield context.call_activity(
        "HkmaMarketPerformanceReportActivity",
        params,
    )

    return True


main = df.Orchestrator.create(orchestrator_function)
