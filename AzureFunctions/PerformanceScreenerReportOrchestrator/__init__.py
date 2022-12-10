import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    client_input: dict = context.get_input()
    params = client_input["params"]

    peer_groups = ["GCM Asia",
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
                   # "GCM ESG",
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

    parallel_period_tasks = []
    for peer_group in peer_groups:
        params.update({"run": "PerformanceScreenerReport"})
        params.update({"peer_group": peer_group})
        peer_params = {"params": params, "data": {}}

        parallel_period_tasks.append(
            context.call_activity(
                "PerformanceScreenerReportActivity",
                peer_params,
            )
        )

    yield context.task_all(parallel_period_tasks)

    return True


main = df.Orchestrator.create(orchestrator_function)
