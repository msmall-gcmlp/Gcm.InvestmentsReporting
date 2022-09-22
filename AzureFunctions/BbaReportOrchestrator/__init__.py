import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    client_input: dict = context.get_input()
    params = client_input["params"]

    parallel_period_tasks = []
    for periodicity in ['ITD', 'YTD']:
        params.update({"run": "EofRbaReport"})
        params.update({"periodicity": periodicity})
        period_params = {"params": params, "data": {}}

        parallel_period_tasks.append(
            context.call_activity(
                "EofRbaReportActivity",
                period_params,
            )
        )

    yield context.task_all(parallel_period_tasks)

    return True


main = df.Orchestrator.create(orchestrator_function)
