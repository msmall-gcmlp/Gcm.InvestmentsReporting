from gcm.inv.utils.azure.durable_functions.base_activity import (
    BaseActivity,
)
import datetime as dt
from ..Reporting.Reports.controller import get_report_class_by_name
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


class ReportConstructorActivity(BaseActivity):
    __json_location = (
        "/".join(["cleansed", "investmentsreporting", "jsonoutputs"]) + "/"
    )

    def __init__(self):
        super().__init__()

    @property
    def parg_type(self):
        return ReportingParsedArgs

    def validate(self, **kwargs) -> Tuple[ReportStructure, ReportMeta]:
        # below will fail if something went wrong in the parser
        report: ReportStructure = get_report_class_by_name(
            self.pargs.ReportName
        )
        available_metas: AvailableMetas = report.available_metas()
        agg: AggregateInterval = Scenario.get_attribute(
            "aggregate_interval"
        )
        agg = agg if (agg is not None) else AggregateInterval.Multi
        assert agg in available_metas.aggregate_intervals
        as_of_date: dt.date = Scenario.get_attribute("as_of_date")
        frequency_type: FrequencyType = Scenario.get_attribute("frequency")

        # we MUST run on today
        assert as_of_date is not None
        frequencies: List[Frequency] = available_metas.frequencies
        # check if we're running on the right date?
        final_freq: Frequency = None
        for f in frequencies:
            if BusinessCalendar().is_business_day(as_of_date, f.calendar):
                final_freq = Frequency(frequency_type, f.calendar)
                break
        assert final_freq is not None
        # now check entity tags:
        domain: EntityDomainTypes = Scenario.get_attribute(
            "EntityDomainTypes"
        )
        if available_metas.entity_groups is not None:
            assert (
                domain is not None
                and domain in available_metas.entity_groups
            )
        return (
            report,
            ReportMeta(
                available_metas.report_type,
                agg,
                final_freq,
                available_metas.consumer,
                domain,
                self._d,
            ),
        )

    def activity(self, **kwargs):
        [report_structure, meta] = self.validate(**kwargs)
        report: ReportStructure = report_structure(meta)
        j = report.to_json()
        dao: DaoRunner = Scenario.get_attribute("dao")
        dl_location = AzureDataLakeDao.create_get_data_params(
            ReportConstructorActivity.__json_location,
            report.base_json_name,
        )
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
