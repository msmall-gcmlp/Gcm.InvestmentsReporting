import azure.durable_functions as df
import json


def orchestrator_function(context: df.DurableOrchestrationContext):
    requestBody: str = context.get_input()
    inputs_request = requestBody.copy()
    inputs_request = json.dumps(inputs_request)
    fund_names = yield context.call_activity("PerformanceQualityInputsActivity", inputs_request)

    parallel_tasks = []
    for fund in fund_names[0:20]:
        params = requestBody.copy()
        params['params']['fund_name'] = fund
        params = json.dumps(params)
        parallel_tasks.append(context.call_activity(
            "PerformanceQualityReportActivity", params
        ))
    yield context.task_all(parallel_tasks)


main = df.Orchestrator.create(orchestrator_function)
