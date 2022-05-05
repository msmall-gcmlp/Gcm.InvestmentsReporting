import azure.durable_functions as df
import pandas as pd
import numpy as np


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
    funds_chunked = np.array_split(funds, round(len(funds)/30, 0))

    for fund_chunk in funds_chunked:
        parallel_tasks = []
        for fund in fund_chunk:
            requestBody['params']['fund_name'] = fund
            parallel_tasks.append(context.call_activity(
                "ReportingActivity", requestBody
            ))
        yield context.task_all(parallel_tasks)


main = df.Orchestrator.create(orchestrator_function)
