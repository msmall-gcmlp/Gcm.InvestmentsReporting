from gcm.inv.utils.azure.durable_functions.base_activity import (
    BaseActivity,
)
from ..utils.reporting_parsed_args import ReportingParsedArgs, ReportNames
import json
from gcm.inv.scenario import Scenario, DaoRunner
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import (
    AzureDataLakeFile,
)
from gcm.Dao.DaoRunner import DaoSource
from ..Reporting.core.report_structure import ReportStructure
from ..Reporting.Reports.controller import get_report_class_by_name
from ..utils.print_utils import print


class ReportPublishActivity(BaseActivity):
    # TODO: Decide if these should be in RAW

    def __init__(self):
        super().__init__()
        self.excel_template_cache = {}

    @property
    def parg_type(self):
        return ReportingParsedArgs

    def activity(self, **kwargs):
        data = json.loads(kwargs["context"])["d"]["data"]
        params_vals = []
        # TODO: figure out if can parallelize the excel stuff
        for i in data:
            # get data from dao:
            dao: DaoRunner = Scenario.get_attribute("dao")
            file: AzureDataLakeFile = dao.execute(
                params=json.loads(i),
                source=DaoSource.DataLake,
                operation=lambda d, p: d.get_data(p),
            )
            assert file is not None
            d = json.loads(file.content)
            r = ReportNames[d["report_name"]]
            report_structure_class = get_report_class_by_name(r)
            report_structure: ReportStructure = (
                report_structure_class.from_dict(d, report_name=r)
            )
            output = print(
                report_structure=report_structure, print_pdf=True
            )
            params_vals.append(output)
        return params_vals


def main(context):
    return ReportPublishActivity().execute(context=context)
