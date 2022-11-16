from gcm.inv.utils.azure.durable_functions.base_activity import (
    BaseActivity,
)
from ..utils.reporting_parsed_args import (
    ReportingParsedArgs,
)


class LegacyReportConstructorActivity(BaseActivity):
    def __init__(self):
        super().__init__()

    @property
    def parg_type(self):
        return ReportingParsedArgs

    def activity(self, **kwargs):
        return True

def main(context):
    return LegacyReportConstructorActivity().execute(context=context)
