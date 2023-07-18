from typing import List
from functools import cached_property, reduce
import pandas as pd
from enum import Enum
from gcm.inv.utils.date.AggregateInterval import AggregateInterval
import numpy as np
from ....pvm_performance_utils.analytics.standards import (
    calc_irr,
    calc_multiple,
    calc_dpi,
    calc_sum,
)


def generate_fund_rpt_dict(
    fund_attrib: pd.DataFrame,
    investment_cf: pd.DataFrame,
    deal_attrib: pd.DataFrame,
    deal_cf: pd.DataFrame,
):
    gross_metrics = [
        # PvmPerformanceResults.MeasureNames.HoldingPeriod,
        PvmPerformanceResults.MeasureNames.EquityInvested,
        PvmPerformanceResults.MeasureNames.UnrealizedValueGross,
        PvmPerformanceResults.MeasureNames.TotalValue,
        PvmPerformanceResults.MeasureNames.InvestmentGain,
        PvmPerformanceResults.MeasureNames.GrossMultiple,
        PvmPerformanceResults.MeasureNames.PctTotalGain,
        PvmPerformanceResults.MeasureNames.LossRatio,
        PvmPerformanceResults.MeasureNames.GrossIrr,
    ]
    net_metrics = [
        PvmPerformanceResults.MeasureNames.NetMultiple,
        PvmPerformanceResults.MeasureNames.NetIrr,
        PvmPerformanceResults.MeasureNames.NetDpi,
    ]

    full_investment_tr_total = get_tr_stats(
        rpt_dimn=fund_attrib[["InvestmentName"]],
        group_cols=["InvestmentName"],
        gross_metrics=gross_metrics,
        net_metrics=net_metrics,
        investment_dimn=fund_attrib,
        investment_cf=investment_cf,
        position_dimn=deal_attrib,
        position_cf=deal_cf,
    )

    realized_investment_tr = get_tr_stats(
        rpt_dimn=deal_attrib[["AssetName", "InvestmentDate", "ExitDate"]],
        group_cols=["AssetName"],
        gross_metrics=gross_metrics,
        net_metrics=net_metrics,
        investment_dimn=fund_attrib,
        investment_cf=investment_cf,
        position_dimn=deal_attrib[deal_attrib.Status == "Realized"],
        position_cf=deal_cf[
            deal_cf.AssetName.isin(
                deal_attrib[deal_attrib.Status == "Realized"].AssetName
            )
        ],
    )
    realized_investment_tr_total = get_tr_stats(
        rpt_dimn=fund_attrib[["InvestmentName"]],
        group_cols=["InvestmentName"],
        gross_metrics=gross_metrics,
        net_metrics=net_metrics,
        investment_dimn=fund_attrib,
        investment_cf=investment_cf,
        position_dimn=deal_attrib[deal_attrib.Status == "Realized"],
        position_cf=deal_cf[
            deal_cf.AssetName.isin(
                deal_attrib[deal_attrib.Status == "Realized"].AssetName
            )
        ],
    )

    unrealized_investment_tr = get_tr_stats(
        rpt_dimn=deal_attrib[["AssetName", "InvestmentDate", "ExitDate"]],
        group_cols=["InvestmentName"],
        gross_metrics=gross_metrics,
        net_metrics=net_metrics,
        investment_dimn=fund_attrib,
        investment_cf=investment_cf,
        position_dimn=deal_attrib[deal_attrib.Status == "Unrealized"],
        position_cf=deal_cf[
            deal_cf.AssetName.isin(
                deal_attrib[deal_attrib.Status == "Unrealized"].AssetName
            )
        ],
    )
    unrealized_investment_tr_total = get_tr_stats(
        rpt_dimn=fund_attrib[["InvestmentName"]],
        group_cols=["InvestmentName"],
        gross_metrics=gross_metrics,
        net_metrics=net_metrics,
        investment_dimn=fund_attrib,
        investment_cf=investment_cf,
        position_dimn=deal_attrib[deal_attrib.Status == "Unrealized"],
        position_cf=deal_cf[
            deal_cf.AssetName.isin(
                deal_attrib[deal_attrib.Status == "Unrealized"].AssetName
            )
        ],
    )

    rpt_rslt = {
        "realized_fund1": realized_investment_tr,
        "realized_fund_total1": realized_investment_tr_total,
        "unrealized_fund1": unrealized_investment_tr,
        "unrealized_fund_total1": unrealized_investment_tr_total,
        "full_fund_total1": full_investment_tr_total,
        # 'fund_relevant_peer' + str(fund_number + 1): fund_relevant_peer,
        "fund_name1": pd.DataFrame(
            {"InvestmentName": fund_attrib.InvestmentName.unique()}
        ),
    }

    return rpt_rslt


def get_perf_concentration_rpt_dict(
    deal_attrib: pd.DataFrame, deal_cf: pd.DataFrame
):
    # only done at position level atm
    # don't love this, but it's all report-specific as it has to be

    concen_realized_rpt, concen_all_rpt = [
        get_perf_concentration_rpt(
            rpt_dimn=deal_attrib[
                ["AssetName", "InvestmentName", "InvestmentDate"]
            ],
            group_cols=["Rank"],
            df=deal_attrib,
            cf=deal_cf,
            realized_only=r,
        )
        for r in [True, False]
    ]

    concen_realized_total_rpt, concen_all_total_rpt = [
        get_perf_concentration_rpt(
            rpt_dimn=deal_attrib[
                ["InvestmentManagerName"]
            ].drop_duplicates(),
            group_cols=["InvestmentManagerName"],
            df=deal_attrib,
            cf=deal_cf,
            realized_only=r,
        )
        for r in [True, False]
    ]

    rpt_groups = [["CostType"], ["InvestmentManagerName"]]
    (
        realized_distrib,
        all_distrib,
        realized_distrib_total,
        all_distrib_total,
    ) = [
        get_distrib_returns_rpt(
            group_cols=c, df=deal_attrib, cf=deal_cf, realized_only=r
        )
        for c in rpt_groups
        for r in [True, False]
    ]

    rpt_groups = [["RankGroup"], ["InvestmentManagerName"]]
    (
        return_concentration_rlzed,
        return_concentration,
        return_concentration_rlzed_total,
        return_concentration_total,
    ) = [
        get_return_concentration_rpt(
            group_cols=c, df=deal_attrib, cf=deal_cf, realized_only=r
        )
        for c in rpt_groups
        for r in [True, False]
    ]

    rpt_dict = {
        "top_deals": concen_all_rpt,
        "top_deals_total": concen_all_total_rpt,
        "top_realized_deals": concen_realized_rpt,
        "top_realized_deals_total": concen_realized_total_rpt,
        "all_distrib": all_distrib,
        "all_distrib_total": all_distrib_total,
        "realized_distrib": realized_distrib,
        "realized_distrib_total": realized_distrib_total,
        "all_concen": return_concentration,
        "all_concen_total": return_concentration_total,
        "realized_concen": return_concentration_rlzed,
        "realized_concen_total": return_concentration_rlzed_total,
    }

    return rpt_dict


def get_return_concentration_rpt(
    group_cols: List[str],
    df: pd.DataFrame,
    cf: pd.DataFrame,
    realized_only=False,
):
    gross_metrics = [
        PvmPerformanceResults.MeasureNames.GrossIrr,
        PvmPerformanceResults.MeasureNames.GrossMultiple,
        PvmPerformanceResults.MeasureNames.PctRealizedGain,
        PvmPerformanceResults.MeasureNames.PctTotalGain,
    ]
    df[PvmPerformanceResults.MeasureNames.PctTotalGain.value] = (
        df.InvestmentGain / df.InvestmentGain.sum()
    )
    df[
        PvmPerformanceResults.MeasureNames.PctRealizedGain.value
    ] = np.where(
        df.Status == "Realized",
        (
            df.InvestmentGain
            / df[df.Status == "Realized"].InvestmentGain.sum()
        ),
        None,
    )

    if realized_only:
        df = df[df.Status == "Realized"]

    df[PvmPerformanceResults.MeasureNames.PctCapital.value] = (
        df.EquityInvested / df.EquityInvested.sum()
    )

    df["Rank"] = (
        df.sort_values("InvestmentGain", ascending=False)
        .reset_index()
        .sort_values("index")
        .index
        + 1
    )
    if group_cols == ["RankGroup"]:
        df["Rank"] = np.where(
            df.Rank.isin([1, 2, 3, 4, 5]), df.Rank, "Other"
        )
        df = pd.concat(
            [
                df[df.Rank == "1"].assign(RankGroup="Top 1"),
                df[df.Rank.isin(["1", "2", "3"])].assign(
                    RankGroup="Top 3"
                ),
                df[df.Rank.isin(["1", "2", "3", "4", "5"])].assign(
                    RankGroup="Top 5"
                ),
                df[df.Rank == "Other"].assign(RankGroup="Other"),
            ]
        )

        cf = cf.merge(
            df[["AssetName", "RankGroup"]],
            how="left",
            left_on="AssetName",
            right_on="AssetName",
        )

    perf_rslt = PvmPerformanceResults(
        cleaned_cashflows=None, aggregate_interval=AggregateInterval.ITD
    )

    rpt_stats = reduce(
        lambda left, right: pd.merge(
            left, right, on=group_cols, how="outer"
        ),
        [
            perf_rslt.get_metric(
                attribute_df=df,
                cashflow_df=cf,
                metric=metric,
                group_cols=group_cols,
            )
            for metric in gross_metrics
        ],
    )
    if not realized_only:
        rpt_stats[
            PvmPerformanceResults.MeasureNames.PctRealizedGain.value
        ] = None
    if group_cols == ["RankGroup"]:
        rpt_stats = (
            rpt_stats.set_index("RankGroup")
            .reindex(["Top 1", "Top 3", "Top 5", "Other"])
            .reset_index()
        )

    return rpt_stats


# DT: TODO: make this work without AssetName
def get_distrib_returns_rpt(
    group_cols: List[str],
    df: pd.DataFrame,
    cf: pd.DataFrame,
    realized_only=False,
):
    gross_metrics = [
        PvmPerformanceResults.MeasureNames.NumInvestments,
        PvmPerformanceResults.MeasureNames.GrossMultiple,
        PvmPerformanceResults.MeasureNames.GrossIrr,
        PvmPerformanceResults.MeasureNames.PctCapital,
    ]
    # distribution of returns
    if realized_only:
        df = df[df.Status == "Realized"]
        cf = cf[cf.AssetName.isin(df[df.Status == "Realized"].AssetName)]

    df[PvmPerformanceResults.MeasureNames.PctTotalGain.value] = (
        df.InvestmentGain / df.InvestmentGain.sum()
    )
    df[PvmPerformanceResults.MeasureNames.PctCapital.value] = (
        df.EquityInvested / df.EquityInvested.sum()
    )

    perf_rslt = PvmPerformanceResults(
        cleaned_cashflows=None, aggregate_interval=AggregateInterval.ITD
    )

    rpt_stats = reduce(
        lambda left, right: pd.merge(
            left, right, on=group_cols, how="outer"
        ),
        [
            perf_rslt.get_metric(
                attribute_df=df,
                cashflow_df=cf,
                metric=metric,
                group_cols=group_cols,
            )
            for metric in gross_metrics
        ],
    )

    return rpt_stats


def get_perf_concentration_rpt(
    rpt_dimn: pd.DataFrame,
    group_cols: List[str],
    df: pd.DataFrame,
    cf: pd.DataFrame,
    realized_only=False,
):
    gross_metrics = [
        PvmPerformanceResults.MeasureNames.EquityInvested,
        PvmPerformanceResults.MeasureNames.UnrealizedValueGross,
        PvmPerformanceResults.MeasureNames.TotalValue,
        PvmPerformanceResults.MeasureNames.InvestmentGain,
        PvmPerformanceResults.MeasureNames.GrossMultiple,
        PvmPerformanceResults.MeasureNames.GrossIrr,
        PvmPerformanceResults.MeasureNames.PctRealizedGain,
        PvmPerformanceResults.MeasureNames.PctTotalGain,
    ]

    df[PvmPerformanceResults.MeasureNames.PctTotalGain.value] = (
        df.InvestmentGain / df.InvestmentGain.sum()
    )
    df[
        PvmPerformanceResults.MeasureNames.PctRealizedGain.value
    ] = np.where(
        df.Status == "Realized",
        (
            df.InvestmentGain
            / df[df.Status == "Realized"].InvestmentGain.sum()
        ),
        None,
    )
    df["PctCapital"] = df.EquityInvested / df.EquityInvested.sum()
    if realized_only:
        df = df[df.Status == "Realized"]
        cf = cf[cf.AssetName.isin(df[df.Status == "Realized"].AssetName)]

    df[PvmPerformanceResults.MeasureNames.PctCapital.value] = (
        df.EquityInvested / df.EquityInvested.sum()
    )

    df["Rank"] = (
        df.sort_values(
            PvmPerformanceResults.MeasureNames.InvestmentGain.value,
            ascending=False,
        )
        .reset_index()
        .sort_values("index")
        .index
        + 1
    )
    df["Rank"] = np.where(df.Rank.isin([1, 2, 3, 4, 5]), df.Rank, "Other")

    cf_ranked = cf.merge(
        df[["AssetName", "Rank"]],
        how="left",
        left_on="AssetName",
        right_on="AssetName",
    )
    assert len(cf_ranked) == len(cf)

    perf_rslt = PvmPerformanceResults(
        aggregate_interval=AggregateInterval.ITD
    )

    rpt_stats = reduce(
        lambda left, right: pd.merge(
            left, right, on=group_cols, how="outer"
        ),
        [
            perf_rslt.get_metric(
                attribute_df=df,
                cashflow_df=cf_ranked,
                metric=metric,
                group_cols=group_cols,
            )
            for metric in gross_metrics
        ],
    )
    if ~realized_only:
        rpt_stats[
            PvmPerformanceResults.MeasureNames.PctRealizedGain.value
        ] = None
    # DT note: below is too confusing and tied to Position Dimn/Cashflows and column names
    if group_cols == ["Rank"]:
        asset_name_order = df[df.Rank != "Other"].sort_values(
            ["Rank"]
        ).AssetName.to_list() + ["Other"]
        rpt_stats["Rank"] = asset_name_order
        rpt_stats.rename(columns={"Rank": "AssetName"}, inplace=True)
        rpt_stats = (
            rpt_dimn[rpt_dimn.AssetName.isin(asset_name_order)]
            .merge(
                rpt_stats,
                how="outer",
                left_on="AssetName",
                right_on="AssetName",
            )
            .set_index("AssetName")
            .reindex(asset_name_order)
            .reset_index()
        )

    return rpt_stats


def get_mgr_rpt_dict(
    manager_attrib: pd.DataFrame,
    fund_attrib: pd.DataFrame,
    investment_cf: pd.DataFrame,
    deal_attrib: pd.DataFrame,
    deal_cf: pd.DataFrame,
):
    gross_metrics = [
        PvmPerformanceResults.MeasureNames.NumInvestments,
        PvmPerformanceResults.MeasureNames.EquityInvested,
        PvmPerformanceResults.MeasureNames.RealizedValueGross,
        PvmPerformanceResults.MeasureNames.UnrealizedValueGross,
        PvmPerformanceResults.MeasureNames.TotalValue,
        PvmPerformanceResults.MeasureNames.GrossMultiple,
        PvmPerformanceResults.MeasureNames.GrossIrr,
        PvmPerformanceResults.MeasureNames.LossRatio,
    ]
    net_metrics = [
        PvmPerformanceResults.MeasureNames.NetMultiple,
        PvmPerformanceResults.MeasureNames.NetIrr,
        PvmPerformanceResults.MeasureNames.NetDpi,
    ]

    full_manager_tr = get_tr_stats(
        rpt_dimn=fund_attrib[
            ["InvestmentName", "VintageYear", "CommittedCapital"]
        ],
        group_cols=["InvestmentName"],
        gross_metrics=gross_metrics,
        net_metrics=net_metrics,
        investment_dimn=fund_attrib,
        investment_cf=investment_cf,
        position_dimn=deal_attrib,
        position_cf=deal_cf,
    )
    full_manager_tr_total = get_tr_stats(
        rpt_dimn=manager_attrib[["InvestmentManagerName"]],
        group_cols=["InvestmentManagerName"],
        gross_metrics=gross_metrics,
        net_metrics=net_metrics,
        investment_dimn=fund_attrib,
        investment_cf=investment_cf,
        position_dimn=deal_attrib,
        position_cf=deal_cf,
    )

    realized_manager_tr = get_tr_stats(
        rpt_dimn=fund_attrib[
            ["InvestmentName", "VintageYear", "CommittedCapital"]
        ],
        group_cols=["InvestmentName"],
        gross_metrics=gross_metrics,
        net_metrics=net_metrics,
        investment_dimn=fund_attrib,
        investment_cf=investment_cf,
        position_dimn=deal_attrib[deal_attrib.Status == "Realized"],
        position_cf=deal_cf[
            deal_cf.AssetName.isin(
                deal_attrib[deal_attrib.Status == "Realized"].AssetName
            )
        ],
    )
    realized_manager_tr_total = get_tr_stats(
        rpt_dimn=manager_attrib[["InvestmentManagerName"]],
        group_cols=["InvestmentManagerName"],
        manager_dimn=manager_attrib,
        gross_metrics=gross_metrics,
        net_metrics=net_metrics,
        investment_dimn=fund_attrib,
        investment_cf=investment_cf,
        position_dimn=deal_attrib[deal_attrib.Status == "Realized"],
        position_cf=deal_cf[
            deal_cf.AssetName.isin(
                deal_attrib[deal_attrib.Status == "Realized"].AssetName
            )
        ],
    )

    unrealized_manager_tr = get_tr_stats(
        rpt_dimn=fund_attrib[
            ["InvestmentName", "VintageYear", "CommittedCapital"]
        ],
        group_cols=["InvestmentName"],
        gross_metrics=gross_metrics,
        net_metrics=net_metrics,
        investment_dimn=fund_attrib,
        investment_cf=investment_cf,
        position_dimn=deal_attrib[deal_attrib.Status == "Unrealized"],
        position_cf=deal_cf[
            deal_cf.AssetName.isin(
                deal_attrib[deal_attrib.Status == "Unrealized"].AssetName
            )
        ],
    )
    unrealized_manager_tr_total = get_tr_stats(
        rpt_dimn=manager_attrib[["InvestmentManagerName"]],
        group_cols=["InvestmentManagerName"],
        gross_metrics=gross_metrics,
        net_metrics=net_metrics,
        investment_dimn=fund_attrib,
        investment_cf=investment_cf,
        position_dimn=deal_attrib[deal_attrib.Status == "Unrealized"],
        position_cf=deal_cf[
            deal_cf.AssetName.isin(
                deal_attrib[deal_attrib.Status == "Unrealized"].AssetName
            )
        ],
    )

    rpt_dict = {
        "full_manager_tr": full_manager_tr,
        "full_manager_tr_total": full_manager_tr_total,
        "realized_manager_tr": realized_manager_tr,
        "realized_manager_tr_total": realized_manager_tr_total,
        "unrealized_manager_tr": unrealized_manager_tr,
        "unrealized_manager_tr_total": unrealized_manager_tr_total,
    }
    return rpt_dict


def calc_loss_ratio(df: pd.DataFrame, group_cols: List[str]):
    df["LossRatio"] = np.where(
        df.InvestmentGain < 0,
        df.InvestmentGain.abs() / df.EquityInvested,
        0,
    )
    loss_ratio = (
        abs(df[df.LossRatio != 0].groupby(group_cols).InvestmentGain.sum())
        / df.groupby(group_cols).EquityInvested.sum()
    )
    rslt = loss_ratio.reset_index().rename(columns={0: "LossRatio"})
    return rslt


def get_tr_stats(
    group_cols: List[str],
    rpt_dimn: pd.DataFrame,
    gross_metrics: List[Enum],
    net_metrics: List[Enum],
    investment_dimn: pd.DataFrame,
    investment_cf: pd.DataFrame,
    position_dimn: pd.DataFrame,
    position_cf: pd.DataFrame,
):
    # all group_cols must be in rpt_dimn columns
    assert (
        len([col for col in group_cols if col not in rpt_dimn.columns])
        == 0
    )
    position_dimn[
        PvmPerformanceResults.MeasureNames.NumInvestments.value
    ] = 1

    perf_rslt = PvmPerformanceResults(
        aggregate_interval=AggregateInterval.ITD
    )
    gross_stats = reduce(
        lambda left, right: pd.merge(
            left, right, on=group_cols, how="outer"
        ),
        [
            perf_rslt.get_metric(
                attribute_df=position_dimn,
                cashflow_df=position_cf,
                metric=metric,
                group_cols=group_cols,
            )
            for metric in gross_metrics
        ],
    )
    net_stats = reduce(
        lambda left, right: pd.merge(
            left, right, on=group_cols, how="outer"
        ),
        [
            perf_rslt.get_metric(
                attribute_df=investment_dimn,
                cashflow_df=investment_cf,
                metric=metric,
                group_cols=group_cols,
            )
            for metric in net_metrics
        ],
    )

    result = reduce(
        lambda left, right: pd.merge(
            left, right, on=group_cols, how="outer"
        ),
        [rpt_dimn, gross_stats, net_stats],
    )
    return result


class PvmPerformanceResults(object):
    def __init__(
        self,
        aggregate_interval: AggregateInterval,
    ):
        self.aggregate_interval = aggregate_interval

    class MeasureNames(Enum):
        HoldingPeriod = "HoldingPeriod"
        CommittedCapital = "CommittedCapital"
        NumInvestments = "NumInvestments"
        EquityInvested = "EquityInvested"
        UnrealizedValueGross = "UnrealizedValueGross"
        RealizedValueGross = "RealizedValueGross"
        TotalValue = "TotalValue"
        InvestmentGain = "InvestmentGain"
        GrossMultiple = "GrossMultiple"
        NetMultiple = "NetMultiple"
        GrossIrr = "GrossIrr"
        NetIrr = "NetIrr"
        NetDpi = "NetDpi"
        PctRealizedGain = "PctRealizedGain"
        PctTotalGain = "PctTotalGain"
        PctCapital = "PctCapital"
        LossRatio = "LossRatio"

    @cached_property
    def summable_metrics(self):
        return [
            PvmPerformanceResults.MeasureNames.NumInvestments,
            PvmPerformanceResults.MeasureNames.EquityInvested,
            PvmPerformanceResults.MeasureNames.RealizedValueGross,
            PvmPerformanceResults.MeasureNames.UnrealizedValueGross,
            PvmPerformanceResults.MeasureNames.TotalValue,
            PvmPerformanceResults.MeasureNames.InvestmentGain,
            PvmPerformanceResults.MeasureNames.PctTotalGain,
            PvmPerformanceResults.MeasureNames.PctRealizedGain,
            PvmPerformanceResults.MeasureNames.PctCapital,
        ]

    def get_metric(
        self,
        metric: Enum,
        attribute_df: pd.DataFrame,
        cashflow_df: pd.DataFrame,
        group_cols: List[str],
    ):

        if metric in self.summable_metrics:
            return calc_sum(
                df=attribute_df,
                group_cols=group_cols,
                sum_col=metric.value,
            )[group_cols + [metric.value]]
        if metric == PvmPerformanceResults.MeasureNames.GrossIrr:
            return calc_irr(
                cf=cashflow_df, group_cols=group_cols, type="Gross"
            )[group_cols + [metric.value]]
        if metric == PvmPerformanceResults.MeasureNames.GrossMultiple:
            return calc_multiple(
                cf=cashflow_df, group_cols=group_cols, type="Gross"
            )[group_cols + [metric.value]]
        if metric == PvmPerformanceResults.MeasureNames.NetMultiple:
            return calc_multiple(
                cf=cashflow_df, group_cols=group_cols, type="Net"
            )[group_cols + [metric.value]]
        if metric == PvmPerformanceResults.MeasureNames.NetIrr:
            return calc_irr(
                cf=cashflow_df, group_cols=group_cols, type="Net"
            )[group_cols + [metric.value]]
        if metric == PvmPerformanceResults.MeasureNames.NetDpi:
            return calc_dpi(
                cf=cashflow_df, group_cols=group_cols, type="Net"
            )[group_cols + [metric.value]]
        if metric == PvmPerformanceResults.MeasureNames.LossRatio:
            return calc_loss_ratio(df=attribute_df, group_cols=group_cols)
        else:
            raise NotImplementedError(f"{metric} not implemented")
