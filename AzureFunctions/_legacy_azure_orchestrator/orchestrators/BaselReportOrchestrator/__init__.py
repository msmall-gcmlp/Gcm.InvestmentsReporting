import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    # get factor Returns
    client_input: dict = context.get_input()
    params = client_input["params"]
    disct1 = {
        "params": {
            "run": "RunBaselReport",
            "as_of_date": params["as_of_date"],
            "balancedate": params["balancedate"],
            "portfolio_name": params["portfolio_name"],
        },
    }

    tables = yield context.call_activity("BaselReportActivity", disct1)

    tables


main = df.Orchestrator.create(orchestrator_function)
