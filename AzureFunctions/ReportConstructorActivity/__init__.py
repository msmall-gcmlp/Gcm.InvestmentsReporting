from gcm.inv.utils.azure.durable_functions.base_activity import (
    BaseActivity,
)
import datetime as dt
from ..Reporting.Reports.controller import (
    get_report_class_by_name,
    validate_meta,
)
from ..Reporting.core.report_structure import (
    AggregateInterval,
    AvailableMetas,
    Frequency,
    List,
    ReportStructure,
    ReportMeta,
    EntityDomainTypes,
)
from typing import Tuple
from gcm.inv.utils.date.Frequency import FrequencyType
from ..utils.reporting_parsed_args import (
    ReportingParsedArgs,
)
from gcm.inv.scenario import Scenario
from gcm.inv.utils.date.business_calendar import BusinessCalendar
from gcm.Dao.DaoRunner import DaoRunner, AzureDataLakeDao, DaoSource
import json
import pandas as pd
import copy


class ReportConstructorActivity(BaseActivity):
    __json_location = AzureDataLakeDao.BlobFileStructure(
        zone=AzureDataLakeDao.BlobFileStructure.Zone.cleansed,
        sources="investmentsreporting",
        entity="jsonoutputs",
        path=[],
    )

    def __init__(self):
        super().__init__()

    @property
    def parg_type(self):
        return ReportingParsedArgs

    def construct_meta(self) -> Tuple[ReportStructure, ReportMeta]:
        # below will fail if something went wrong in the parser
        report: ReportStructure = get_report_class_by_name(
            self.pargs.ReportName
        )
        available_metas: AvailableMetas = report.available_metas()
        agg: AggregateInterval = Scenario.get_attribute(
            "aggregate_interval"
        )
        agg = agg if (agg is not None) else AggregateInterval.Multi
        as_of_date: dt.date = Scenario.get_attribute("as_of_date")
        # TODO: speed this up and make better
        frequency_type: FrequencyType = Scenario.get_attribute("frequency")
        frequencies: List[Frequency] = available_metas.frequencies
        for f in frequencies:
            if BusinessCalendar().is_business_day(as_of_date, f.calendar):
                final_freq = Frequency(frequency_type, f.calendar)
                break
        domain: EntityDomainTypes = Scenario.get_attribute(
            "EntityDomainTypes"
        )
        entity_info: pd.DataFrame = None
        if self._d is not None:
            dict_of_pargs = json.loads(self._d)
            if "entity" in dict_of_pargs:
                entity_info: pd.DataFrame = pd.read_json(
                    dict_of_pargs["entity"]
                )
        return (
            report,
            ReportMeta(
                available_metas.report_type,
                agg,
                final_freq,
                available_metas.consumer,
                domain,
                entity_info,
            ),
        )

    def activity(self, **kwargs):
        [report_structure, meta] = self.construct_meta()
        validate_meta(report_meta=meta, report_structure=report_structure)
        report: ReportStructure = report_structure(meta)
        j = report.to_json()
        dao: DaoRunner = Scenario.get_attribute("dao")
        dl_location = copy.deepcopy(
            ReportConstructorActivity.__json_location
        )
        dl_location.path.append(report.base_json_name)
        dl_location = AzureDataLakeDao.create_blob_params(dl_location)
        dl_location = {
            key: value
            for key, value in dl_location.items()
            if key in ["filesystem_name", "file_path", "retry", "metadata"]
        }
        if self.pargs.save:

            dao.execute(
                params=dl_location,
                source=DaoSource.DataLake,
                operation=lambda runner, p: runner.post_data(p, j),
            )
        return json.dumps(dl_location)


def main(context):
    return ReportConstructorActivity().execute(context=context)
