import azure.functions as func


async def main(mytimer: func.TimerRequest, starter: str) -> None:
    # client = df.DurableOrchestrationClient(starter)
    # orchestrator = "ReportingOrchestrator"
    # instance_id = await client.start_new(orchestrator, None, None)

    # logging.info(f"Started orchestration of {orchestrator}" +
    #              f"with ID = '{instance_id}'.")
    pass
