import azure.durable_functions as df
import logging
import json


def orchestrator_function(context: df.DurableOrchestrationContext):
    client_input: dict = context.get_input()
    params = client_input["params"]

    first_retry_interval_in_milliseconds = 600000
    max_number_of_attempts = 3
    retry_options = df.RetryOptions(first_retry_interval_in_milliseconds, max_number_of_attempts)

    logging.info("Collecting underlying report data")
    data_params = params.copy()
    data_params.update({"run": "PerformanceQualityReportData"})
    data_params = {"params": data_params, "data": {}}

    funds_and_peers = \
        yield context.call_activity_with_retry("PerformanceQualityReportActivity", retry_options, data_params)

    funds_and_peers = json.loads(funds_and_peers)

    logging.info("Generating peer group summaries")
    parallel_peer_tasks = []
    peer_groups = funds_and_peers.get('peer_groups')

    for peer in peer_groups:
        peer_params = params.copy()
        peer_params.update({"run": "PerformanceQualityPeerSummaryReport"})
        peer_params.update({"peer_group": peer})
        peer_params = {"params": peer_params, "data": {}}
        parallel_peer_tasks.append(context.call_activity_with_retry(
            "PerformanceQualityReportActivity", retry_options, peer_params
        ))
    yield context.task_all(parallel_peer_tasks)

    logging.info("Generating fund summaries")
    parallel_fund_tasks = []
    fund_names = funds_and_peers.get('fund_names')

    for fund in fund_names:
        report_params = params.copy()
        report_params.update({"run": "PerformanceQualityReport"})
        report_params.update({"fund_name": fund})
        report_params = {"params": report_params, "data": {}}
        parallel_fund_tasks.append(context.call_activity_with_retry(
            "PerformanceQualityReportActivity", retry_options, report_params
        ))
    yield context.task_all(parallel_fund_tasks)

    return True


main = df.Orchestrator.create(orchestrator_function)
