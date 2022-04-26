import azure.durable_functions as df
import pandas as pd


def orchestrator_function(context: df.DurableOrchestrationContext):
    requestBody: str = context.get_input()
    requestBody['params']['run'] = "PerformanceQualityReportData"
    fund_dimn, fund_monthly_returns, emms = yield context.call_activity(
        "ReportingActivity", requestBody
    )

    requestBody['params']['run'] = "PerformanceQualityReport"
    requestBody['params']['fund_dimn'] = fund_dimn
    requestBody['params']['fund_monthly_returns'] = fund_monthly_returns

    emm_dimns = pd.read_json(emms, orient='index')
    emm_names = emm_dimns['InvestmentGroupName'].values
    
    for fund in emm_names:
        requestBody['params']['fund_name'] = fund
        
        yield context.call_activity(
            "ReportingActivity", requestBody
        )
    return True
    


main = df.Orchestrator.create(orchestrator_function)
