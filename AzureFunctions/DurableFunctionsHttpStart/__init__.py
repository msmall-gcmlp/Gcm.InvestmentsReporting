import logging
import azure.functions as func
import azure.durable_functions as df
from ..utils.reporting_parsed_args import (
    ReportingParsedArgs,
    ExtendedEntityPargs,
)
from gcm.inv.utils.azure.durable_functions.parg_serialization import (
    serialize_pargs,
)
from ..legacy_azure_orchestrator.legacy_orchestrations import (
    LegacyOrchestrations,
    LegacyOrchestrationNoParsedArgs,
)
from ..legacy_azure_orchestrator.legacy_report_orch_parsed_args import (
    LegacyReportingOrchParsedArgs,
)
import json


async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)

    function_name = req.route_params["functionName"]
    pargs: ExtendedEntityPargs = None
    params = req.params
    body = req.get_body().decode()
    requestBody = json.loads("{ }" if body == "" else body)
    d = {}
    for k in params.keys():
        d[k] = params[k]
    client_input = {"params": d, "data": requestBody}

    legacy_orchestrator_names = [
        LegacyOrchestrations(x).name for x in LegacyOrchestrations.list()
    ]
    legacy_names_no_parsed_args = [
        LegacyOrchestrationNoParsedArgs(x).name for x in LegacyOrchestrationNoParsedArgs.list()
    ]
    if function_name not in legacy_names_no_parsed_args:
        if function_name in legacy_orchestrator_names:
            function_name = "legacy_azure_orchestrator"
            pargs = LegacyReportingOrchParsedArgs.from_http(req)
        else:
            pargs = ReportingParsedArgs.from_http(req)
        client_input = serialize_pargs(pargs, client_input)
    instance_id = await client.start_new(
        function_name,
        client_input=client_input,
    )
    logging.info(
        f"Started orchestration. ID: '{instance_id}'. "
        + f"Function: '{function_name}'"
    )

    return client.create_check_status_response(req, instance_id)
