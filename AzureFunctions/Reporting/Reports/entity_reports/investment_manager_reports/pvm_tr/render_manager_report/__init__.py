from gcm.inv.models.pvm.node_evaluation.evaluation_item import (
    PvmNodeEvaluatable,
)
from ....xentity_reports.pvm_tr.base_render import (
    RenderTablesRenderer,
    ReportWorksheet,
    BaseRenderer,
)
from ......core.components.report_worksheet import (
    ReportTable,
)
from typing import List
import pandas as pd
from gcm.inv.models.pvm.node_evaluation.evaluation_item.to_df import (
    simple_display_df_from_evaluatable as df_evaluate,
)
from gcm.inv.models.pvm.node_evaluation.evaluation_provider.df_utils import (
    ATOMIC_COUNT,
    atomic_node_count,
)
from gcm.inv.utils.pvm.node.utils import PvmNodeBase
from gcm.Dao.DaoRunner import AzureDataLakeDao
from gcm.inv.utils.pvm.standard_mappings import (
    KnownRealizationStatus,
)
from typing import NamedTuple, Union
from gcm.inv.dataprovider.entity_provider.controller import (
    EntityDomainTypes,
)

TEMPLATE = AzureDataLakeDao.BlobFileStructure(
        zone=AzureDataLakeDao.BlobFileStructure.Zone.raw,
        sources="investmentsreporting",
        entity="exceltemplates",
        path=["PvmManagerTrackRecordTemplate.xlsx"]
    )

class InvBreakout(NamedTuple):
    Investment: pd.DataFrame
    Total: pd.DataFrame


class GrossBreakoutTables(BaseRenderer):
    Evaluated_Columns = [
        PvmNodeEvaluatable.PvmEvaluationType.cost,
        PvmNodeEvaluatable.PvmEvaluationType.realized_value,
        PvmNodeEvaluatable.PvmEvaluationType.unrealized_value,
        PvmNodeEvaluatable.PvmEvaluationType.total_value,
        PvmNodeEvaluatable.PvmEvaluationType.moic,
        PvmNodeEvaluatable.PvmEvaluationType.irr,
        PvmNodeEvaluatable.PvmEvaluationType.loss_ratio,
    ]
    _PERCENT_OF_TOTAL = PvmNodeEvaluatable.PvmEvaluationType.pnl

    @classmethod
    def _reference_columns(cls) -> List[str]:
        return super()._reference_columns() + [ATOMIC_COUNT]

    @classmethod
    def _evaluated_columns_to_show_in_df(cls):
        return [x.name for x in cls.Evaluated_Columns]

    @classmethod
    def _percentage_columns(cls):
        return []

    @classmethod
    def include_atomic_count(cls):
        return True

    @classmethod
    def type_col(cl):
        return "Gross"

    @classmethod
    def generate(
        cls,
        breakout: List[PvmNodeEvaluatable],
    ) -> pd.DataFrame:
        df = []
        for i in breakout:
            this_df = df_evaluate(i, cls.Evaluated_Columns)
            if cls.include_atomic_count():
                this_df[ATOMIC_COUNT] = int(atomic_node_count(i))
            df.append(this_df)
        df = pd.concat(df)
        df.reset_index(inplace=True, drop=True)
        # NO need to do this because
        # Gavin wants the count as a seperate column
        # df = cls.generate_updated_diplay_name(df)

        df = cls.render_with_final_columns(df)
        rename = {}
        for k in cls.Evaluated_Columns:
            if k.name in df.columns:
                rename[k.name] = f"{cls.type_col()} {k.name}"
        df.rename(columns=rename, inplace=True)
        return df


class NetBreakoutTables(GrossBreakoutTables):
    Evaluated_Columns = [
        PvmNodeEvaluatable.PvmEvaluationType.moic,
        PvmNodeEvaluatable.PvmEvaluationType.irr,
        PvmNodeEvaluatable.PvmEvaluationType.dpi,
    ]

    @classmethod
    def _reference_columns(cls) -> List[str]:
        return BaseRenderer._reference_columns()

    @classmethod
    def type_col(cl):
        return "Net"

    @classmethod
    def include_atomic_count(cls):
        return False


class RenderRealizationStatusFundBreakout_NetGross(RenderTablesRenderer):
    def __init__(
        self,
        gross_realization_status_breakout: List[PvmNodeEvaluatable],
        net_breakout: List[PvmNodeEvaluatable],
    ):
        self.gross_realization_status_breakout = (
            gross_realization_status_breakout
        )
        self.net_breakout = net_breakout

    def _generate_gross_dfs(
        self, items=List[PvmNodeEvaluatable]
    ) -> pd.DataFrame:
        df = GrossBreakoutTables.generate(items)
        return df

    def _generate_net_dfs(
        self, items=List[PvmNodeEvaluatable]
    ) -> pd.DataFrame:
        df = NetBreakoutTables.generate(items)
        return df

    _TOTAL = "Total"

    def get_gross_breakout(
        self,
    ) -> dict[Union[KnownRealizationStatus, str], InvBreakout]:
        cls = self.__class__
        realization_status_dict: dict[
            Union[KnownRealizationStatus, str], InvBreakout
        ] = {}
        dict_of_funds: dict[str, List[PvmNodeEvaluatable]] = {}
        for r in self.gross_realization_status_breakout:
            fund_breakout = self._generate_gross_dfs(r.children)
            total_for_realization_bucket = self._generate_gross_dfs([r])

            for fund in r.children:
                # assumption is that naming convention is the same
                # can do more explicit comparison if this fs up
                if fund.display_name in dict_of_funds:
                    base_item = dict_of_funds[fund.display_name]
                    base_item.append(fund)
                    dict_of_funds[fund.display_name] = base_item
                else:
                    dict_of_funds[fund.display_name] = [fund]

                realization_status_dict[
                    KnownRealizationStatus[r.display_name]
                ] = InvBreakout(
                    fund_breakout, total_for_realization_bucket
                )

        all_gross = []
        fund_cache: List[PvmNodeEvaluatable] = []
        for f, v in dict_of_funds.items():
            inv_node = PvmNodeEvaluatable(
                f, node_type=EntityDomainTypes.Investment.name, children=v
            )
            fund_cache.append(inv_node)
            all_gross = all_gross + v
        f_fund_gross = self._generate_gross_dfs(fund_cache)
        all = PvmNodeEvaluatable(
            cls._TOTAL,
            node_type=EntityDomainTypes.InvestmentManager.name,
            children=all_gross,
        )
        f_manager_all = self._generate_gross_dfs([all])

        realization_status_dict[cls._TOTAL] = InvBreakout(
            f_fund_gross, f_manager_all
        )
        return realization_status_dict

    def get_net_breakout(self) -> InvBreakout:
        df = self._generate_net_dfs(self.net_breakout)
        total = PvmNodeEvaluatable(
            self.__class__._TOTAL,
            node_type=EntityDomainTypes.InvestmentManager.name,
            children=self.net_breakout,
        )
        total_df = self._generate_net_dfs([total])
        return InvBreakout(df, total_df)

    def render(self) -> ReportWorksheet:
        gross_broken_out = self.get_gross_breakout()
        all_net = self.get_net_breakout()
        to_render: List[ReportTable] = []
        trim = []
        for k, v in gross_broken_out.items():
            inv_df = v.Investment
            total_df = v.Total
            if k == self.__class__._TOTAL:
                inv_df = pd.merge(
                    inv_df,
                    all_net.Investment,
                    on=PvmNodeBase._DISPLAY_NAME,
                    how="left",
                )
                total_df = pd.merge(
                    total_df,
                    all_net.Total,
                    on=PvmNodeBase._DISPLAY_NAME,
                    how="left",
                )
            render_name = k if type(k) == str else k.name
            fund_named_range = f"{render_name}_inv"
            total_named_range = f"{render_name}_total"
            to_render.append(ReportTable(fund_named_range, inv_df))
            to_render.append(ReportTable(total_named_range, total_df))
            trim.append(fund_named_range)

        ws = ReportWorksheet(
            "Manager TR",
            ReportWorksheet.ReportWorkSheetRenderer(trim_region=trim),
            to_render,
        )
        return ws
