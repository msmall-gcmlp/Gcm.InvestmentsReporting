from .....core.report_structure import (
    ReportStructure,
    ReportMeta,
    AvailableMetas,
)
from gcm.inv.utils.date.AggregateInterval import AggregateInterval
from .....core.report_structure import (
    ReportType,
    ReportConsumer,
    EntityDomainTypes,
    EntityStandardNames,
)
from gcm.inv.utils.date.Frequency import Frequency, FrequencyType
from ....report_names import ReportNames
from ..PvmTrackRecord.data_handler.pvm_track_record_handler import (
    TrackRecordHandler,
    TrackRecordManagerProvider,
)
from abc import abstractclassmethod, abstractproperty

# http://localhost:7071/orchestrators/ReportOrchestrator?as_of_date=2022-09-30&ReportName=PvmManagerTrackRecordReport&frequency=Once&save=True&aggregate_interval=ITD&EntityDomainTypes=InvestmentManager&EntityNames=[%22ExampleManagerName%22]


class BasePvmTrackRecordReport(ReportStructure):
    def __init__(self, r_type: ReportNames, report_meta: ReportMeta):
        super().__init__(r_type, report_meta)

    IDW_PVM_TR = "IDW.PVM.TR"

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

    @property
    def idw_pvm_tr_id(self):
        __name = "__idw_pvm_tr_id"
        if getattr(self, __name, None) is None:
            info = self.report_meta.entity_info
            info = info[
                [
                    EntityStandardNames.SourceName,
                    EntityStandardNames.ExternalId,
                ]
            ]
            info = info[
                info[EntityStandardNames.SourceName]
                == BasePvmTrackRecordReport.IDW_PVM_TR
            ]
            id_list = [
                int(x)
                for x in info[EntityStandardNames.ExternalId]
                .drop_duplicates()
                .to_list()
            ]
            assert len(id_list) == 1
            setattr(self, __name, id_list[0])
        return getattr(self, __name, None)

    @abstractclassmethod
    def level(cls) -> EntityDomainTypes:
        pass

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
            entity_groups=[cls.level()],
        )
