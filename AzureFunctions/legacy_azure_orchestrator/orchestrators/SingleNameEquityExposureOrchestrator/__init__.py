import azure.durable_functions as df
from ..legacy_tasks import LegacyTasks, ActivitySet, ActivityParams
from ...legacy_report_orch_parsed_args import LegacyReportingOrchParsedArgs


def orchestrator_function(
    context: df.DurableOrchestrationContext,
) -> LegacyTasks:
    # get factor Returns
    orchestrator_input: dict = (
        LegacyReportingOrchParsedArgs.parse_client_inputs(context)
    )
    # """
    # run param can take following values
    # 'RunSingleNameEquityExposureInvestmentGroupPersist'
    # 'RunSingleNameEquityExposureInvestmentGroupReport'
    # 'RunSingleNameEquityExposurePortfolio'
    # 'RunSingleNameEquityExposureSummary'
    # """
    return LegacyTasks(
        [
            ActivitySet(
                [ActivityParams("SingleNameEquityActivity", orchestrator_input)]
            )
        ]
    )
