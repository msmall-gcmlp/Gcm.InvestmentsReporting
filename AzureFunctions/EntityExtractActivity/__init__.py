from gcm.inv.dataprovider.entity_provider.azure_extension.extended_entity_activity import (
    ExtendedEntityExtractActivity,
)
import pandas as pd
from gcm.inv.dataprovider.entity_provider.entity_domains.base.entity_domain_provider import (
    EntityDomainProvider,
)
from ..utils.reporting_parsed_args import ReportingParsedArgs
from ..Reporting.Reports.controller import (
    get_report_class_by_name,
    ReportStructure,
)


def main(context):
    return ExtendedReportSpecificEntityExtractActivity().execute(
        context=context
    )


class ExtendedReportSpecificEntityExtractActivity(
    ExtendedEntityExtractActivity
):
    def __init__(self):
        super().__init__()

    @property
    def parg_type(self):
        return ReportingParsedArgs

    def default_get(self, domain: EntityDomainProvider) -> pd.DataFrame:
        report_name = self.pargs.ReportName
        report: ReportStructure = get_report_class_by_name(report_name)
        func = report.standard_entity_get_callable(
            domain, pargs=self.pargs
        )
        df: pd.DataFrame = func()
        return df
