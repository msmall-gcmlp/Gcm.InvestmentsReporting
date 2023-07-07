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
from ..pvm_track_record.data_handler.pvm_track_record_handler import (
    TrackRecordHandler,
    TrackRecordManagerSingletonProvider,
)
from abc import abstractclassmethod, abstractproperty
from functools import cached_property

# http://localhost:7071/orchestrators/ReportOrchestrator?as_of_date=2022-09-30&ReportName=PvmManagerTrackRecordReport&frequency=Once&save=True&aggregate_interval=ITD&EntityDomainTypes=InvestmentManager&EntityNames=[%22ExampleManagerName%22]


class BasePvmTrackRecordReport(ReportStructure):
    def __init__(self, r_type: ReportNames, report_meta: ReportMeta):
        super().__init__(r_type, report_meta)

    @abstractproperty
    def manager_name(self) -> str:
        raise NotImplementedError()

    @property
    def manager_handler(self) -> TrackRecordHandler:
        # doesn't need to be cached as is already accessing singleton
        manager_handler = (
            TrackRecordManagerSingletonProvider().get_manager_tr_info(
                self.manager_name
            )
        )
        return manager_handler

    @cached_property
    def idw_pvm_tr_id(self) -> int:
        info = self.report_meta.entity_info
        info = info[
            [
                EntityStandardNames.SourceName,
                EntityStandardNames.ExternalId,
            ]
        ]
        # info = info[
        #     info[EntityStandardNames.SourceName]
        #     == BasePvmTrackRecordReport.__IDW_PVM_TR
        # ]
        # DT: hard code
        info = info[
            info[EntityStandardNames.SourceName] == "PVM.TR.Investment.Id"
        ]
        id_list = [
            int(x)
            for x in info[EntityStandardNames.ExternalId]
            .drop_duplicates()
            .to_list()
        ]
        assert len(id_list) == 1
        return id_list[0]

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
                vertical=ReportConsumer.Vertical.PE,
            ),
            entity_groups=[cls.level()],
        )
