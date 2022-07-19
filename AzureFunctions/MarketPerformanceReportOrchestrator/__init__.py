import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    # get factor Returns
    client_input: dict = context.get_input()
    params = client_input["params"]
    disct1 = {
            "params": {
                "run": "RunMarketPerformanceQualityReportData",
                "asofdate": params['asofdate']
            },
    }

    tables = yield context.call_activity(
        "MarketPerformanceReportActivity", disct1
    )

    tables


main = df.Orchestrator.create(orchestrator_function)
