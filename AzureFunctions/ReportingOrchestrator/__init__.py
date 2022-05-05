import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    requestBody: str = context.get_input()
    requestBody['params']['run'] = "PerformanceQualityReportData"
    fund_names = yield context.call_activity("ReportingActivity", requestBody)

    requestBody['params']['run'] = "PerformanceQualityReport"

    parallel_tasks = []
    for fund in fund_names:
        requestBody['params']['fund_name'] = fund
        parallel_tasks.append(context.call_activity(
            "ReportingActivity", requestBody
        ))
    yield context.task_all(parallel_tasks)


main = df.Orchestrator.create(orchestrator_function)
