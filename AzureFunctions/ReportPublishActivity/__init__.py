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
from typing import List
from ..Reporting.core.components.report_workbook_handler import (
    ReportWorkBookHandler,
)
from .utils.standard_handler import (
    generate_workbook,
    Workbook,
    merge_files,
    print_excel_report,
)


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
            assert report_structure is not None
            [params, source] = report_structure.save_params()

            # TODO: Not sure why this happens with ReportingStorage
            f_s = "filesystem_name"
            if (
                source == DaoSource.ReportingStorage
                and type(params) == dict
                and f_s in params
            ):
                params[f_s] = params[f_s].strip("/")

            params = {
                key: value
                for key, value in params.items()
                if key
                in [
                    "filesystem_name",
                    "file_path",
                    "retry",
                    "metadata",
                ]
            }
            params_vals.append(params)
            known_components: List[
                ReportWorkBookHandler
            ] = report_structure.components
            assert all(
                [
                    type(x) == ReportWorkBookHandler
                    for x in known_components
                ]
            )
            wbs: List[Workbook] = []
            for k in known_components:
                wb = generate_workbook(k)
                wbs.append(wb)
            final_wb = merge_files(wbs)
            print_excel_report(
                final_wb,
                dao,
                source,
                params,
                Scenario.get_attribute("save"),
            )
        return json.dumps(params_vals)


def main(context):
    return ReportPublishActivity().execute(context=context)
