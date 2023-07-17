from .....core.report_structure import (
    ReportStructure,
    ReportMeta,
    AvailableMetas,
)
from typing import List
from gcm.inv.utils.date.AggregateInterval import AggregateInterval
from .....core.report_structure import (
    ReportType,
    ReportConsumer,
    EntityDomainTypes,
    EntityStandardNames,
)
from gcm.inv.utils.date.Frequency import Frequency, FrequencyType
from ....report_names import ReportNames
from abc import abstractclassmethod, abstractproperty
from functools import cached_property
from ...utils.pvm_performance_results.attribution import (
    PvmTrackRecordAttribution,
)
from ..pvm_performance_results.report_layer_results import (
    ReportingLayerAggregatedResults,
)
from enum import Enum, auto
import pandas as pd
from gcm.inv.dataprovider.entity_data.investment_manager.pvm.tr import TrackRecordManagerProvider, TrackRecordHandler


# http://localhost:7071/orchestrators/ReportOrchestrator?as_of_date=2022-09-30&ReportName=PvmManagerTrackRecordReport&frequency=Once&save=True&aggregate_interval=ITD&EntityDomainTypes=InvestmentManager&EntityNames=[%22ExampleManagerName%22]


class BasePvmTrackRecordReport(ReportStructure):
    def __init__(self, r_type: ReportNames, report_meta: ReportMeta):
        super().__init__(r_type, report_meta)

    __IDW_PVM_TR = "IDW.PVM.TR"

    @abstractproperty
    def manager_name(self) -> str:
        raise NotImplementedError()

    @property
    def manager_handler(self) -> TrackRecordHandler:
        # doesn't need to be cached as is already accessing singleton
        manager_handler = (
            TrackRecordManagerProvider().get_manager_tr_info(
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
        info = info[
            info[EntityStandardNames.SourceName]
            == BasePvmTrackRecordReport.__IDW_PVM_TR
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

    @abstractproperty
    def pvm_perfomance_results(self) -> PvmTrackRecordAttribution:
        raise NotImplementedError()

    @cached_property
    def __realization_status_breakout(
        self,
    ) -> ReportingLayerAggregatedResults:
        results = self.pvm_perfomance_results
        attribution = results.position_attribution(
            [
                TrackRecordHandler.CommonPositionAttribution.RealizationStatus.name
            ]
        ).results()
        return attribution

    @property
    def total_positions_line_item(
        self,
    ) -> ReportingLayerAggregatedResults:
        item = self.__realization_status_breakout
        return item

    class _KnownRealizationStatusBuckets(Enum):
        REALIZED = auto()
        UNREALIZED = auto()

    def _get_realization_bucket(
        self, bucket: _KnownRealizationStatusBuckets, include_unknown=False
    ):
        layers = self.__realization_status_breakout.sub_layers

        def simple_compare(
            x: ReportingLayerAggregatedResults,
            b: BasePvmTrackRecordReport._KnownRealizationStatusBuckets,
        ):
            _name = x.name
            upp = _name.upper()
            upp = upp.replace("RealizationStatus -".upper(), "")
            upp = upp.strip()
            return upp.startswith(b.name.upper())

        true_vals = [x for x in layers if simple_compare(x, bucket)]
        final_bucket: List[ReportingLayerAggregatedResults] = []
        if include_unknown:
            remaining = [i for i in layers]
            for i in [
                e
                for e in BasePvmTrackRecordReport._KnownRealizationStatusBuckets
            ]:
                remaining = [
                    x for x in remaining if (not simple_compare(x, i))
                ]

            final_bucket = list(set(true_vals + remaining))
        else:
            final_bucket = true_vals
        if len(final_bucket) > 1:
            computed_names = " & ".join(
                list(set([x.name for x in final_bucket]))
            )
            item = ReportingLayerAggregatedResults(
                computed_names,
                sub_layers=final_bucket,
                aggregate_interval=self.report_meta.interval,
            )
            return item
        elif len(final_bucket) == 1:
            return final_bucket[0]
        elif len(final_bucket) == 0:
            return None
        else:
            raise RuntimeError()

    GroupOtherWithRealized = False

    @property
    def realized_reporting_layer(
        self,
    ) -> ReportingLayerAggregatedResults:
        group_other_in_realized = self.__class__.GroupOtherWithRealized
        return self._get_realization_bucket(
            BasePvmTrackRecordReport._KnownRealizationStatusBuckets.REALIZED,
            group_other_in_realized,
        )

    @property
    def unrealized_reporting_layer(
        self,
    ) -> ReportingLayerAggregatedResults:
        group_other_in_realized = not self.__class__.GroupOtherWithRealized
        return self._get_realization_bucket(
            BasePvmTrackRecordReport._KnownRealizationStatusBuckets.UNREALIZED,
            group_other_in_realized,
        )

    @property
    def performance_details(self) -> "PerformanceDetailsHelper":
        return PerformanceDetailsHelper(self)


class PerformanceDetailsHelper(object):
    def __init__(self, report: BasePvmTrackRecordReport):
        self.report = report

    # return Other for 5 only.
    _1_3_5 = [(1, False), (3, False), (5, True)]

    @staticmethod
    def _1_3_5_objects(
        layer_item: ReportingLayerAggregatedResults,
    ) -> dict[object, ReportingLayerAggregatedResults]:
        final = {}
        if layer_item is not None:
            for i in PerformanceDetailsHelper._1_3_5:
                length = i[0]
                return_other = i[1]
                [
                    output,
                    other,
                ] = layer_item.get_position_performance_concentration_at_layer(
                    length, return_other=return_other
                )
                final[i[0]] = output
                if other is not None:
                    final[-1 * i[0]] = other
        return final

    @staticmethod
    def _1_3_5_object_to_df(
        _1_3_5_object: dict[object, ReportingLayerAggregatedResults]
    ) -> pd.DataFrame:
        final = []
        for k, v in _1_3_5_object.items():
            final.append(v.to_df())
        final = pd.concat(final)
        final.reset_index(inplace=True, drop=True)

    @staticmethod
    def get_1_3_5_other_df(
        layer_item: ReportingLayerAggregatedResults,
    ) -> pd.DataFrame:

        results = PerformanceDetailsHelper._1_3_5_objects(layer_item)
        return PerformanceDetailsHelper._1_3_5_object_to_df(results)

    @staticmethod
    def get_performance_distribution(
        layer_item: ReportingLayerAggregatedResults,
    ) -> ReportingLayerAggregatedResults:

        return (
            layer_item
            if layer_item is None
            else layer_item.get_return_distributions_at_layer()
        )
