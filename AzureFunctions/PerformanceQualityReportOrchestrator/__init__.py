import azure.durable_functions as df
import json


def orchestrator_function(context: df.DurableOrchestrationContext):
    requestBody: str = context.get_input()
    inputs_request = requestBody.copy()
    inputs_request = json.dumps(inputs_request)
    funds_and_peers = yield context.call_activity("PerformanceQualityInputsActivity", inputs_request)

    funds_and_peers = json.loads(funds_and_peers)
    fund_names = funds_and_peers.get('fund_names')
    peer_groups = funds_and_peers.get('peer_groups')

    parallel_peer_tasks = []
    for peer in peer_groups:
        params = requestBody.copy()
        params['params']['peer_group'] = peer
        params = json.dumps(params)
        parallel_peer_tasks.append(context.call_activity(
            "PerformanceQualityPeerSummaryActivity", params
        ))
    yield context.task_all(parallel_peer_tasks)

    funds_chunked = [fund_names[i:i + 50] for i in range(0, len(fund_names), 50)]

    for fund_chunk in funds_chunked:
        parallel_fund_tasks = []
        for fund in fund_chunk:
            params = requestBody.copy()
            params['params']['fund_name'] = fund
            params = json.dumps(params)
            parallel_fund_tasks.append(context.call_activity(
                "PerformanceQualityReportActivity", params
            ))
        yield context.task_all(parallel_fund_tasks)

    return True


main = df.Orchestrator.create(orchestrator_function)
