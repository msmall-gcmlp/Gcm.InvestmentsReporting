import azure.durable_functions as df
from gcm.inv.utils.azure.durable_functions.base_orchestrator import (
    BaseOrchestrator,
)
from gcm.inv.utils.azure.durable_functions.parg_serialization import (
    serialize_pargs,
)
from ..utils.reporting_parsed_args import (
    ReportingParsedArgs,
)
import pandas as pd


class ReportOrchestrator(BaseOrchestrator):
    def __init__(self):
        super().__init__()

    @property
    def parg_type(self):
        return ReportingParsedArgs

    def orchestrate(self, context: df.DurableOrchestrationContext):
        # first - get entities (activity)
        entities = yield context.call_activity(
            "EntityExtractActivity",
            serialize_pargs(self.pargs),
        )
        df = pd.read_json(entities)
        assert df is not None
        provisioning_tasks = []
        for index, row in df.iterrows():
            row_json = row.to_json()
            provision_task = context.call_sub_orchestrator(
                "ReportRunnerOrchestrator",
                serialize_pargs(self.pargs, row_json),
            )
            provisioning_tasks.append(provision_task)
        data_location = yield context.task_all(provisioning_tasks)
        publish_location = yield context.call_activity(
            "ReportPublishActivity",
            serialize_pargs(self.pargs, {"data": data_location}),
        )
        return publish_location
        # next, execute reportrunner sub-orchestrator


main = df.Orchestrator.create(ReportOrchestrator.main)
