import azure.durable_functions as df
import json


def orchestrator_function(context: df.DurableOrchestrationContext):
    client_input: dict = context.get_input()
    params = client_input["params"]

    data_params = params.copy()
    data_params.update({"run": "PerformanceQualityReportData"})
    data_params = {"params": data_params, "data": {}}

    funds_and_peers = \
        yield context.call_activity("PerformanceQualityReportActivity", data_params)

    funds_and_peers = json.loads(funds_and_peers)
    fund_names = funds_and_peers.get('fund_names')
    peer_groups = funds_and_peers.get('peer_groups')

    parallel_peer_tasks = []
    for peer in peer_groups:
        params.update({"run": "PerformanceQualityPeerSummaryReport"})
        params.update({"peer_group": peer})
        peer_params = {"params": params, "data": {}}
        parallel_peer_tasks.append(context.call_activity(
            "PerformanceQualityReportActivity", peer_params
        ))
    yield context.task_all(parallel_peer_tasks)

    parallel_fund_tasks = []
    for fund in fund_names:
        params.update({"run": "PerformanceQualityReport"})
        params.update({"fund_name": fund})
        report_params = {"params": params, "data": {}}
        parallel_fund_tasks.append(context.call_activity(
            "PerformanceQualityReportActivity", report_params
        ))
    yield context.task_all(parallel_fund_tasks)

    return True


main = df.Orchestrator.create(orchestrator_function)
