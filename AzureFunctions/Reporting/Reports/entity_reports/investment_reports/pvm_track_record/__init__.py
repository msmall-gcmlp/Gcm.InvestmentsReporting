from .....core.report_structure import (
    ReportStructure,
    ReportMeta,
    AvailableMetas,
)
from gcm.inv.utils.date.AggregateInterval import (
    AggregateInterval,
    AggregateIntervalReportHandler,
)
from .....core.report_structure import (
    ReportType,
    ReportConsumer,
    EntityDomainTypes,
    EntityStandardNames,
)
from gcm.inv.models.pvm.node_evaluation.evaluation_provider import (
    PvmEvaluationProvider,
)
from gcm.inv.utils.date.Frequency import Frequency, FrequencyType
from abc import abstractclassmethod, abstractproperty
from functools import cached_property
from enum import Enum
from gcm.inv.dataprovider.entity_data.investment_manager.pvm.tr import (
    TrackRecordManagerProvider,
    TrackRecordHandler,
)
from typing import List
from gcm.Dao.DaoRunner import AzureDataLakeDao
from gcm.inv.models.pvm.node_evaluation.evaluation_provider.from_.pvm_track_record import (
    PvmTrackRecordNodeProvider,
    PvmTrackRecordInvestmentDataContainer,
)
from gcm.inv.dataprovider.entity_data.investment.pvm.tr import (
    Investment_To_ManagerMapping,
    PvmInvestmentTrackRecord as GetPvmInvestmentTrackRecord,
)
from gcm.inv.scenario import Scenario
from gcm.inv.models.pvm.node_evaluation.evaluation_provider.custom_buckets.by_realization_status import (
    generate_realization_status_groups,
    RealizedUnrealizedBreakout,
)
from gcm.inv.scenario import gcm_cell
import pandas as pd


# http://localhost:7071/orchestrators/ReportOrchestrator?as_of_date=2022-09-30&ReportName=PvmManagerTrackRecordReport&frequency=Once&save=True&aggregate_interval=ITD&EntityDomainTypes=InvestmentManager&EntityNames=[%22ExampleManagerName%22]


class BasePvmTrackRecordReport(ReportStructure):
    def __init__(self, report_name: Enum, report_meta: ReportMeta):
        super().__init__(report_name, report_meta)

    @abstractclassmethod
    def level(cls) -> EntityDomainTypes:
        raise NotImplementedError()

    @abstractproperty
    def manager_name(self) -> str:
        raise NotImplementedError()

    @abstractproperty
    def investments(self) -> List[str]:
        raise NotImplementedError()

    @property
    def manager_handler(self) -> TrackRecordHandler:
        # doesn't need to be cached as is already accessing singleton
        manager_handler = TrackRecordManagerProvider().get_manager_tr_info(
            self.manager_name
        )
        return manager_handler

    def evaluated_by_realization_status(
        self,
    ) -> RealizedUnrealizedBreakout:
        positions: PvmEvaluationProvider = (
            self.node_provider.position_tr_node_provider
        )
        return generate_realization_status_groups(positions)

    @gcm_cell
    def node_provider(self) -> PvmTrackRecordNodeProvider:
        return PvmTrackRecordNodeProvider(
            self.scenario_evaluated_investments
        )

    @gcm_cell
    def scenario_evaluated_investments(
        self,
    ) -> List[PvmTrackRecordInvestmentDataContainer]:
        # use data provider to get all scenario objects. nice thing is thiat this gets released
        inputs = [
            Investment_To_ManagerMapping(x, self.manager_name)
            for x in self.investments
        ]
        handler = GetPvmInvestmentTrackRecord(investment_names=inputs)
        as_of_date = Scenario.get_attribute("as_of_date")
        aggregate_interval = Scenario.get_attribute("aggregate_interval")
        return handler.get_scenario_investment_objects(
            as_of_date=as_of_date, aggregate_interval=aggregate_interval
        )

    @cached_property
    def idw_pvm_tr_id(self) -> int:
        info = self.report_meta.entity_info
        info = info[
            [
                EntityStandardNames.SourceName,
                EntityStandardNames.ExternalId,
            ]
        ]
        info = info[
            info[EntityStandardNames.SourceName]
            == TrackRecordHandler.IDW_PVM_TR
        ]
        id_list = [
            int(x)
            for x in info[EntityStandardNames.ExternalId]
            .drop_duplicates()
            .to_list()
        ]
        assert len(id_list) == 1
        return id_list[0]

    @classmethod
    def available_metas(cls):
        return AvailableMetas(
            report_type=ReportType.Performance,
            frequencies=[
                Frequency(FrequencyType.Once),
            ],
            aggregate_intervals=[
                AggregateIntervalReportHandler([AggregateInterval.ITD])
            ],
            consumer=ReportConsumer(
                horizontal=[ReportConsumer.Horizontal.IC],
                vertical=ReportConsumer.Vertical.PE,
            ),
            entity_groups=[cls.level()],
        )

    @cached_property
    def position_to_investment_breakout(self) -> pd.DataFrame:
        positions: PvmEvaluationProvider = (
            self.node_provider.position_tr_node_provider
        )
        investments: PvmEvaluationProvider = (
            self.node_provider.investment_tr_node_provider
        )
        merge_columns = [
            x
            for x in positions.atomic_dimensions.columns
            if x in [y for y in investments.atomic_dimensions.columns]
        ]
        position_select_columns = list(
            set(merge_columns + positions.atomic_df_identifier)
        )
        investment_select_columns = list(
            set(merge_columns + investments.atomic_df_identifier)
        )
        investments: PvmEvaluationProvider = (
            self.node_provider.investment_tr_node_provider
        )
        merged = pd.merge(
            positions.atomic_dimensions[position_select_columns],
            investments.atomic_dimensions[investment_select_columns],
            how="left",
            on=merge_columns,
        )
        merged.drop_duplicates(inplace=True)
        return merged

    @property
    def attribution_template(self):
        return AzureDataLakeDao.BlobFileStructure(
            zone=AzureDataLakeDao.BlobFileStructure.Zone.raw,
            sources="investmentsreporting",
            entity="exceltemplates",
            path=["AttributionResults.xlsx"],
        )
