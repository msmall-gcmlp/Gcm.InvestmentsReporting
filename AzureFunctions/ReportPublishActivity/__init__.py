from gcm.inv.utils.azure.durable_functions.base_activity import (
    BaseActivity,
)
from ..utils.reporting_parsed_args import ReportingParsedArgs, ReportNames
import json
from gcm.inv.scenario import Scenario, DaoRunner
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import (
    AzureDataLakeFile,
    AzureDataLakeDao,
    TabularDataOutputTypes,
)
from gcm.Dao.DaoRunner import DaoSource
from ..Reporting.core.report_structure import ReportStructure
from ..Reporting.Reports.controller import get_report_class_by_name
from azure.core.exceptions import ResourceNotFoundError
from openpyxl import Workbook
from ..utils.conversion_tools.report_structure_to_excel import (
    print_report_to_template,
    save_report_structure_to_excel_return_virtual_book,
    save_pdf_from_workbook_and_structure,
)


class ReportPublishActivity(BaseActivity):
    # TODO: Decide if these should be in RAW

    def __init__(self):
        super().__init__()
        self.excel_template_cache = {}

    @property
    def parg_type(self):
        return ReportingParsedArgs

    def load_report_structure(self, i):
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

    def get_template(self, report_structure: ReportStructure):
        assert report_structure is not None
        dao: DaoRunner = Scenario.get_attribute("dao")
        try:
            params = AzureDataLakeDao.create_blob_params(
                report_structure.excel_template_location,
            )
            file: AzureDataLakeFile = dao.execute(
                params=params,
                source=DaoSource.DataLake,
                operation=lambda d, p: d.get_data(p),
            )
            excel = file.to_tabular_data(
                TabularDataOutputTypes.ExcelWorkBook, params
            )
            return excel
        except ResourceNotFoundError:
            return None

    def activity(self, **kwargs):
        data = json.loads(kwargs["context"])["d"]["data"]
        params_vals = []
        # TODO: figure out if can parallelize the excel stuff
        for i in data:
            # get data from dao:
            report_structure = self.load_report_structure(i)
            template = self.get_template(report_structure)
            if template is None:
                raise NotImplementedError(
                    "We do not support template-less reports yet"
                )
            else:
                template: Workbook = template
                printed_template = print_report_to_template(
                    template, report_structure
                )
                [
                    b,
                    params,
                    source,
                ] = save_report_structure_to_excel_return_virtual_book(
                    printed_template,
                    report_structure,
                    self.pargs.save,
                )
                save_pdf_from_workbook_and_structure(
                    b, report_structure, source, params, self.pargs.save
                )
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
        return json.dumps(params_vals)


def main(context):
    return ReportPublishActivity().execute(context=context)
