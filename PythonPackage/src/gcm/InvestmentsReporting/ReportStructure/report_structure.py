from abc import ABC
from enum import Enum
import logging
from gcm.Scenario.scenario_enums import AggregateInterval
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from gcm.Dao.daos.azure_datalake.azure_datalake_file import (
    AzureDataLakeFile,
)
from gcm.Dao.Utils.tabular_data_util_outputs import TabularDataOutputTypes
from gcm.Dao.DaoRunner import DaoRunner
from gcm.Dao.DaoSources import DaoSource
import openpyxl
from ..Utils.excel_io import ExcelIO
from openpyxl.writer.excel import save_virtual_workbook
import datetime as dt
import json

template_location = (
    "/".join(
        [
            "raw",
            "test",
            "rqstest",
            "rqstest",
            "ReportingTemplates",
        ]
    )
    + "/"
)

base_output_location = (
    "/".join(
        [
            "raw",
            "test",
            "rqstest",
            "rqstest",
            "ReportingTemplates",
        ]
    )
    + "/"
)


class ReportStage(Enum):
    PreDeployment = (0,)
    IC = (1,)
    Active = (2,)
    Legacy = (3,)


class ReportVertical(Enum):
    PEREI = (0,)
    ARS = (1,)
    SIG = (2,)
    FirmWide = (3,)


class ReportType(Enum):
    Risk = (0,)
    CapitalAndExposure = (1,)
    Performance = (2,)
    CommitmentsAndFlows = (3,)


class ReportSubstrategy(Enum):
    Credit = (0,)
    Equities = (1,)
    Primaries = (2,)
    Secondaries = (3,)
    Infrastructure = (4,)
    All = (5,)


class RiskReportConsumer(Enum):
    RiskMonitoring = (0,)
    InternalExRMA = (1,)
    CIO = (2,)
    External = (3,)


# seperate class in case we want to load once
# and pass to multiple report structures
class ReportTemplate(object):
    def __init__(self, filename, runner):
        self.filename = filename
        self._excel = None
        self.runner: DaoRunner = runner

    def excel(self, excel_params: dict = {}) -> openpyxl.Workbook:
        if self._excel is None:
            # standard location

            params = AzureDataLakeDao.create_get_data_params(
                template_location,
                self.filename,
                retry=False,
            )
            file: AzureDataLakeFile = self.runner.execute(
                params=params,
                source=DaoSource.DataLake,
                operation=lambda dao, params: dao.get_data(params),
            )
            self._excel = file.to_tabular_data(
                output_type=TabularDataOutputTypes.ExcelWorkBook,
                params=params,
            )
        return self._excel


# https://gcmlp1.atlassian.net/wiki/spaces/IN/pages/2719186981/Metadata+Fields
class ReportStructure(ABC):
    def __init__(
        self,
        report_name,
        data,
        asofdate,
        runner,
        aggregate_intervals=[AggregateInterval.Daily],
        report_types=[ReportType.Risk],
        stage=ReportStage.Active,
        report_vertical=[ReportVertical.FirmWide],
        report_substrategy=[ReportSubstrategy.All],
        report_consumers=[RiskReportConsumer.RiskMonitoring],
    ):
        self.report_name = report_name
        self.data = data
        self.gcm_as_of_date = asofdate
        self.gcm_report_period = aggregate_intervals
        self.gcm_report_type = report_types
        self.gcm_report_target_stage = stage
        self.gcm_business_group = report_vertical
        self.gcm_strategy = report_substrategy
        self.gcm_target_audience = report_consumers
        self.template: ReportTemplate = None
        self._runner = runner

    def load_template(self, template: ReportTemplate):
        if self.template is None:
            self.template = template
        else:
            logging.log("Template info has already been set")

    def print_data_to_excel(self):
        if self.template is not None:
            wb: openpyxl.Workbook = self.template.excel()
            excel_io = ExcelIO()
            print("going to attempt to print to template")
            for k in self.data:
                address = list(wb.defined_names[k].destinations)
                for sheetname, cell_address in address:
                    cell_address = cell_address.replace("$", "")
                    # override wb:
                    wb = excel_io.write_dataframe_to_xl(
                        wb, self.data[k], sheetname, cell_address
                    )
            params = AzureDataLakeDao.create_get_data_params(
                base_output_location,
                self.output_name(),
                metadata=self.serialize_metadata(),
            )
            b = save_virtual_workbook(wb)
            self._runner.execute(
                params=params,
                source=DaoSource.DataLake,
                operation=lambda dao, params: dao.post_data(params, b),
            )
        else:
            print("printing on df per page")

    def output_name(self):
        return f'{self.report_name}_{self.gcm_as_of_date.strftime("%Y-%m-%d")}.xlsx'

    def serialize_metadata(self):
        # convert tags from above
        # to json-serializable dictionary
        d = {}
        all_data = self.__dict__
        for k in all_data:
            k: str = k
            if k.startswith("gcm_"):
                # we want to serialize this:
                val = all_data[k]
                metadata = None
                if type(val) == dt.datetime:
                    metadata = val.strftime("%Y-%m-%d")
                elif type(val) == list:
                    if len(val) > 0:
                        if all(issubclass(type(f), Enum) for f in val):
                            metadata = json.dumps(
                                list(map(lambda x: x.name, val))
                            )

                elif issubclass(type(val), Enum):
                    metadata = val.name
                elif type(val) == str:
                    metadata = val
                if metadata is not None:
                    d[k] = metadata
        return d
