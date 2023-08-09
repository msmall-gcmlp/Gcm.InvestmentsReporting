import azure.durable_functions as df
from ..legacy_tasks import LegacyTasks, ActivitySet, ActivityParams
from ...legacy_report_orch_parsed_args import LegacyReportingOrchParsedArgs
import ast
import copy


# def orchestrator_function(
#     context: df.DurableOrchestrationContext,
# ) -> LegacyTasks:
#     client_input = LegacyReportingOrchParsedArgs.parse_client_inputs(
#         context
#     )
#     params = client_input["params"]
#     params = {
#         "params": {
#             "run": "XPFundPqReportActivity",
#             "as_of_date": params["as_of_date"],
#         },
#     }
#     return LegacyTasks(
#         [
#             ActivitySet(
#                 [ActivityParams("XPFundPqReportActivity", params)]
#             )
#         ]
#     )

def orchestrator_function(
    context: df.DurableOrchestrationContext,
) -> LegacyTasks:
    # get factor Returns
    client_input = LegacyReportingOrchParsedArgs.parse_client_inputs(
        context
    )
    params = client_input["params"]

    acronyms = params.get("acronyms", [None])
    if (acronyms != [None]):
        acronyms = ast.literal_eval(params.get("acronyms"))

    #acronyms = ast.literal_eval(params.get("acronyms"))
    # TODO support scenario per acronym
    as_of_date = params.get("as_of_date")

    parallel_period_tasks = []
    for acronym in acronyms:
        p = copy.deepcopy(params)
        p.update({"run": "PortfolioConstructionReport"})
        p.update({"as_of_date": as_of_date})
        p.update({"acronym": acronym})
        report_params = {"params": p, "data": {}}
        parallel_period_tasks.append(
            ActivityParams(
                "XPFundPqReportActivity", report_params
            )
        )

    return LegacyTasks([ActivitySet(parallel_period_tasks)])


main = df.Orchestrator.create(orchestrator_function)
