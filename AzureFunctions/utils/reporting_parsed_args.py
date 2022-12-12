from gcm.inv.entityhierarchy.az_func.entity_extract_activity_base import (
    EntityParsedArgs,
)
from ..Reporting.Reports.controller import ReportNames


class ReportingParsedArgs(EntityParsedArgs):
    def __init__(self):
        super().__init__()

    @classmethod
    def from_dict(cls, d: dict):
        pargs = super().from_dict(d)
        if "ReportName" in d:
            if d["ReportName"] in [
                ReportNames(x).name for x in ReportNames.list()
            ]:
                pargs.ReportName = ReportNames[d["ReportName"]]
        else:
            raise NotImplementedError()
        return pargs
