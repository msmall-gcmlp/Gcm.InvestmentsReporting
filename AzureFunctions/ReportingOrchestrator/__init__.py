import azure.durable_functions as df
import pandas as pd


def orchestrator_function(context: df.DurableOrchestrationContext):
    requestBody: str = context.get_input()
    requestBody['params']['run'] = "PerformanceQualityReportData"
    report_inputs = yield context.call_activity(
        "ReportingActivity", requestBody
    )

    report_inputs['vertical'] = requestBody['params']['vertical']
    report_inputs['entity'] = requestBody['params']['entity']
    requestBody['params'] = report_inputs
    requestBody['params']['run'] = "PerformanceQualityReport"

    funds = pd.read_json(report_inputs['fund_dimn'], orient='index')['InvestmentGroupName'].tolist()

    parallel_tasks = []
    for fund in ['D1 Capital', 'Citadel', 'Skye']:
        requestBody['params']['fund_name'] = fund
        parallel_tasks.append(context.call_activity(
            "ReportingActivity", requestBody
        ))
    yield context.task_all(parallel_tasks)
    return True


main = df.Orchestrator.create(orchestrator_function)
