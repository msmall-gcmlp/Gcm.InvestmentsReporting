import azure.durable_functions as df

# http://localhost:7071/orchestrators/ReportCopyOrchestrator?search_by=Performance%20Quality&source_env=uat&source_sub=nonprd&source_zone=performance&


def orchestrator_function(context: df.DurableOrchestrationContext):
    requestBody = context.get_input()
    get_file_list = yield context.call_activity(
        "ReportCopyActivity", requestBody
    )
    return get_file_list


main = df.Orchestrator.create(orchestrator_function)
