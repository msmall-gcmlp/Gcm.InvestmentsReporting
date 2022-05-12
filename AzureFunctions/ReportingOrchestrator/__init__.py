import azure.durable_functions as df
import json


def orchestrator_function(context: df.DurableOrchestrationContext):
    requestBody: str = context.get_input()
    activity1 = requestBody.copy()
    activity1['params']['run'] = "PerformanceQualityReportData"
    activity1 = json.dumps(activity1)
    
    fund_names = yield context.call_activity("ReportingActivity", activity1)

    parallel_tasks = []
    for fund in fund_names:
        activity2 = requestBody.copy()
        activity2['params']['run'] = "PerformanceQualityReport"
        activity2['params']['fund_name'] = fund
        activity2 = json.dumps(activity2)
        parallel_tasks.append(context.call_activity(
            "ReportingActivity", activity2
        ))
    yield context.task_all(parallel_tasks)


main = df.Orchestrator.create(orchestrator_function)
