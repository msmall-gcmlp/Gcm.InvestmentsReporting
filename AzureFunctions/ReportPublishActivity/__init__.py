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
from openpyxl.writer.excel import save_virtual_workbook
from ..utils.excel_io import ExcelIO
from ..Reporting.core.components.report_table import ReportTable
import copy
from ..utils.convert_excel_to_pdf import convert

# from joblib import Parallel, delayed
# import multiprocessing
import io


class ReportPublishActivity(BaseActivity):
    # TODO: Decide if these should be in RAW

    def __init__(self):
        super().__init__()
        self.excel_template_cache = {}

    @property
    def parg_type(self):
        return ReportingParsedArgs

    # def applyParallel(self, dfGrouped, uploaded_source, func):
    #     retLst = Parallel(n_jobs=multiprocessing.cpu_count())(
    #         delayed(func)(name, group, uploaded_source)
    #         for name, group in dfGrouped
    #     )
    #     return pd.concat(retLst)

    def get_template(self, report_structure: ReportStructure):
        assert report_structure is not None
        if (
            report_structure.excel_template
            not in self.excel_template_cache
            or self.excel_template_cache[report_structure.excel_template]
            is not None
        ):

            dao: DaoRunner = Scenario.get_attribute("dao")
            try:
                params = AzureDataLakeDao.create_get_data_params(
                    ReportStructure._excel_template_folder,
                    report_structure.excel_template,
                )
                file: AzureDataLakeFile = dao.execute(
                    params=params,
                    source=DaoSource.DataLake,
                    operation=lambda d, p: d.get_data(p),
                )
                excel = file.to_tabular_data(
                    TabularDataOutputTypes.ExcelWorkBook, params
                )
                self.excel_template_cache[
                    report_structure.excel_template
                ] = excel
            # if the resource was not found
            except ResourceNotFoundError:
                self.excel_template_cache[
                    report_structure.excel_template
                ] = None

        return self.excel_template_cache[report_structure.excel_template]

    @staticmethod
    def print_report_to_template(wb: Workbook, struct: ReportStructure):
        copied = copy.deepcopy(wb)
        for k in [x for x in struct.components if type(x) == ReportTable]:
            if k in copied.defined_names:
                address = list(copied.defined_names[k].destinations)
                for sheetname, cell_address in address:
                    cell_address = cell_address.replace("$", "")
                    # override wb:
                    copied = ExcelIO.write_dataframe_to_xl(
                        copied, k.df, sheetname, cell_address
                    )
        return copied

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
            template = self.get_template(report_structure)
            if template is None:
                # no template, get standard template
                print("reached")
            else:
                template: Workbook = template
                final_template = (
                    ReportPublishActivity.print_report_to_template(
                        template, report_structure
                    )
                )
                b = save_virtual_workbook(final_template)
                [params, source] = report_structure.save_params
                if self.pargs.save:
                    dao.execute(
                        params=params,
                        source=source,
                        operation=lambda d, v: d.post_data(v, b),
                    )
                    convert(
                        io.BytesIO(b),
                        base_params=params,
                        source=source,
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
