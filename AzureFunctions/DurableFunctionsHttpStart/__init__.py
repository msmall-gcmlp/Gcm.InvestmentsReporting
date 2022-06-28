import logging
import json
import azure.functions as func
import azure.durable_functions as df


async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)

    function_name = req.route_params["functionName"]
    body = req.get_body().decode()
    params = req.params
    requestBody = json.loads("{ }" if body == "" else body)
    d = {}
    for k in params.keys():
        d[k] = params[k]
    client_input = {"params": d, "data": requestBody}
    instance_id = await client.start_new(
        function_name, client_input=client_input
    )

    logging.info(
        f"Started orchestration. ID: '{instance_id}'. "
        + f"Function: '{function_name}'"
    )

    return client.create_check_status_response(req, instance_id)
