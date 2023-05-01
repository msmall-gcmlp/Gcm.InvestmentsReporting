from ....core.report_structure import (
    ReportStructure,
    ReportMeta,
    AvailableMetas,
)
from gcm.inv.utils.date.AggregateInterval import AggregateInterval
from ....core.report_structure import (
    ReportType,
    ReportConsumer,
)
from gcm.inv.utils.date.Frequency import Frequency, FrequencyType
from ...report_names import ReportNames
from .pvm_track_record_handler import (
    TrackRecordHandler,
    TrackRecordManagerProvider,
)
from abc import abstractproperty

# http://localhost:7071/orchestrators/ReportOrchestrator?as_of_date=2022-09-30&ReportName=PvmManagerTrackRecordReport&frequency=Once&save=True&aggregate_interval=ITD&EntityDomainTypes=InvestmentManager&EntityNames=[%22ExampleManagerName%22]


class BasePvmTrackRecordReport(ReportStructure):
    def __init__(self, report_meta: ReportMeta):
        super().__init__(ReportNames.BasePvmTrackRecordReport, report_meta)

    @abstractproperty
    def manager_name(self) -> str:
        raise NotImplementedError()

    @property
    def manager_handler(self) -> TrackRecordHandler:
        __name = "__manager_handler"
        __ifc = getattr(self, __name, None)
        if __ifc is None:
            manager_handler = (
                TrackRecordManagerProvider().get_manager_tr_info(
                    self.manager_name
                )
            )
            setattr(self, __name, manager_handler)
        return getattr(self, __name, None)

    @classmethod
    def available_metas(cls):
        return AvailableMetas(
            report_type=ReportType.Performance,
            frequencies=[
                Frequency(FrequencyType.Once),
            ],
            aggregate_intervals=[AggregateInterval.ITD],
            consumer=ReportConsumer(
                horizontal=[ReportConsumer.Horizontal.IC],
                vertical=ReportConsumer.Vertical.PEREI,
            ),
        )
