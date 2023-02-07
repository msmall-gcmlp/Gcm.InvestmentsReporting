import ast

import azure.durable_functions as df
from ..legacy_tasks import LegacyTasks, ActivitySet, ActivityParams
from ...legacy_report_orch_parsed_args import LegacyReportingOrchParsedArgs
import copy


def orchestrator_function(
    context: df.DurableOrchestrationContext,
) -> LegacyTasks:
    # get factor Returns
    client_input = LegacyReportingOrchParsedArgs.parse_client_inputs(
        context
    )
    params = client_input["params"]

    if params.get("peer_groups") is None:
        peer_groups = [
            "GCM Asia",
            # "GCM Asia Credit",
            "GCM Asia Equity",
            # "GCM Asia Macro",
            "GCM China",
            # "GCM Commodities",
            "GCM Consumer",
            "GCM Credit",
            "GCM Cross Cap",
            "GCM Diverse",
            "GCM Diversifying Strategies",
            # "GCM DS Alternative Trend",
            # "GCM DS CTA",
            # "GCM DS Multi-Strategy",
            # "GCM DS Traditional CTA",
            "GCM Emerging Market Credit",
            "GCM Energy",
            # "GCM EOF Comps",
            # "GCM Equities",
            # "GCM Equity plus MN Quant",
            "GCM ESG",
            # "GCM ESG Credit",
            "GCM Europe Credit",
            "GCM Europe Equity",
            "GCM Financials",
            "GCM Fundamental Credit",
            "GCM Generalist Long/Short Equity",
            "GCM Healthcare",
            "GCM Illiquid Credit",
            # "GCM India",
            "GCM Industrials",
            "GCM Japan",
            "GCM Long Only Equity",
            "GCM Long/Short Credit",
            "GCM Macro",
            "GCM Merger Arbitrage",
            "GCM Multi-PM",
            "GCM Multi-Strategy",
            "GCM Quant",
            "GCM Real Estate",
            "GCM Relative Value",
            # "GCM Short Sellers",
            "GCM Structured Credit",
            "GCM TMT",
            "GCM Utilities",
        ]
    else:
        peer_groups = ast.literal_eval(params.get("peer_groups"))

    parallel_period_tasks = []
    for peer_group in peer_groups:
        p = copy.deepcopy(params)
        p.update({"run": "PerformanceScreenerReport"})
        p.update({"peer_group": peer_group})
        peer_params = {"params": p, "data": {}}
        parallel_period_tasks.append(
            ActivityParams(
                "PerformanceScreenerReportActivity", peer_params
            )
        )

    return LegacyTasks([ActivitySet(parallel_period_tasks)])


main = df.Orchestrator.create(orchestrator_function)
