import pandas as pd
import numpy as np
from pyxirr import xirr
from ....pvm_performance_utils.analytics.standards import (
    calc_irr,
    calc_multiple,
)


def generate_fund_rpt_dict(df, cf, _exclude_net=False):
    df_fund = df.copy()
    df_fund["HoldingPeriod"] = (
        df_fund.ExitDate - df_fund.InvestmentDate
    ) / pd.Timedelta("365 days")
    df_fund["LossRatio"] = np.where(
        df_fund.InvestmentGain < 0,
        df_fund.InvestmentGain.abs() / df_fund.EquityInvested,
        0,
    )

    realized = df_fund[df_fund.Status == "Realized"]
    if len(realized) != 0:
        realized_fund, realized_fund_total = _get_single_fund_stats(
            realized, cf=cf
        )
    else:
        realized_fund = pd.DataFrame({"Name": ["No Realized Deals"]})
        realized_fund_total = pd.DataFrame({"Name": ["Total (0)"]})
        # realized_rslt.to_csv('C:/Tmp/' + str(fund) + '_Realized.csv')

    unrealized = df_fund[df_fund.Status == "Unrealized"]
    if len(unrealized) != 0:
        unrealized_fund, unrealized_fund_total = _get_single_fund_stats(
            unrealized, cf=cf
        )
        # unrealized_rslt.to_csv('C:/Tmp/' + str(fund) + '_Unealized.csv')
    else:
        unrealized_fund = pd.DataFrame({"Name": ["No Unrealized Deals"]})
        unrealized_fund_total = pd.DataFrame({"Name": ["Total (0)"]})

    if len(df_fund) != 0:
        all_fund, all_fund_total = _get_single_fund_stats(df_fund, cf=cf)
        # all_rslt.to_csv('C:/Tmp/' + str(fund) + '_All.csv')
    else:
        all_fund = pd.DataFrame({"Name": ["No Deals"]})
        all_fund_total = pd.DataFrame({"Name": ["Total (0)"]})

    rpt_rslt = {
        "realized_fund1": realized_fund,
        "realized_fund_total1": realized_fund_total,
        "unrealized_fund1": unrealized_fund,
        "unrealized_fund_total1": unrealized_fund_total,
        "full_fund_total1": all_fund_total,
        # 'fund_relevant_peer' + str(fund_number + 1): fund_relevant_peer,
        "fund_name1": pd.DataFrame(
            {"InvestmentName": df.InvestmentName.unique()}
        ),
    }

    return rpt_rslt


def _get_single_fund_stats(df, cf):
    df["PctTotalGain"] = df.InvestmentGain / df.InvestmentGain.sum()
    df = df[
        [
            "AssetName",
            "InvestmentDate",
            "ExitDate",
            "HoldingPeriod",
            "EquityInvested",
            "UnrealizedValueGross",
            "TotalValue",
            "InvestmentGain",
            "PctTotalGain",
            "LossRatio",
        ]
    ].reset_index()

    gross_multiple = calc_multiple(
        cf[cf.AssetName.isin(df.AssetName)],
        group_cols=["AssetName"],
        type="Gross",
    )
    gross_irr = calc_irr(
        cf[cf.AssetName.isin(df.AssetName)],
        group_cols=["AssetName"],
        type="Gross",
    )
    deals = df.merge(
        gross_multiple,
        how="left",
        left_on="AssetName",
        right_on="AssetName",
    ).merge(
        gross_irr, how="left", left_on="AssetName", right_on="AssetName"
    )

    # separate primarily because of weighted average of holding period
    total_df = deals.copy()
    total_df["AssetName"] = "Total"
    total_df["alloc"] = (
        total_df.EquityInvested / total_df.EquityInvested.sum()
    )
    total_df.HoldingPeriod = total_df.alloc * total_df.HoldingPeriod

    total = (
        total_df[
            [
                "AssetName",
                "HoldingPeriod",
                "TotalValue",
                "UnrealizedValueGross",
                "InvestmentGain",
                "EquityInvested",
                "PctTotalGain",
            ]
        ]
        .groupby(["AssetName"])
        .sum()
    )
    total["GrossMultiple"] = calc_multiple(
        cf[cf.AssetName.isin(deals.AssetName)],
        group_cols=None,
        type="Gross",
    )
    total["GrossIrr"] = calc_irr(
        cf[cf.AssetName.isin(deals.AssetName)],
        group_cols=None,
        type="Gross",
    )
    total["LossRatio"] = (
        deals[deals.LossRatio != 0].InvestmentGain.abs().sum()
        / deals.EquityInvested.sum()
    )

    rslt = pd.concat(
        [deals.sort_values("InvestmentGain", ascending=False), total]
    )[
        [
            "AssetName",
            "InvestmentDate",
            "ExitDate",
            "HoldingPeriod",
            "EquityInvested",
            "UnrealizedValueGross",
            "TotalValue",
            "InvestmentGain",
            "GrossMultiple",
            "PctTotalGain",
            "LossRatio",
            "GrossIrr",
        ]
    ]

    deal_stats = rslt[rslt.index != "Total"]
    total = rslt[rslt.index == "Total"]
    total["AssetName"] = "Total (" + str(len(deal_stats)) + ")"

    return deal_stats, total


def get_perf_concentration_rpt_dict(deal_attrib, deal_cf):
    input_data = get_perf_concentration_rpt(
        deal_attrib, deal_cf, realized_only=False
    )

    concen_rlzd_rslt = get_perf_concentration_rpt(
        deal_attrib, deal_cf, realized_only=True
    )
    input_data.update(concen_rlzd_rslt)

    distribution_rslt = get_distrib_returns_rpt(
        deal_attrib, deal_cf, realized_only=False
    )
    input_data.update(distribution_rslt)

    distribution_rlzd_rslt = get_distrib_returns_rpt(
        deal_attrib, deal_cf, realized_only=True
    )
    input_data.update(distribution_rlzd_rslt)

    return_concentration = get_return_concentration_rpt(
        deal_attrib, deal_cf, realized_only=False
    )
    input_data.update(return_concentration)

    return_concentration_rlzed = get_return_concentration_rpt(
        deal_attrib, deal_cf, realized_only=True
    )
    input_data.update(return_concentration_rlzed)

    return input_data


def get_return_concentration_rpt(df, cf, realized_only):
    # concentration of returns
    df["PctTotalGain"] = df.InvestmentGain / df.InvestmentGain.sum()
    df["PctRealizedGain"] = (
        df.InvestmentGain
        / df[df.Status == "Realized"].InvestmentGain.sum()
    )
    if realized_only:
        df = df[df.Status == "Realized"]

    df["PctCapital"] = df.EquityInvested / df.EquityInvested.sum()
    df["Rank"] = (
        df.sort_values("InvestmentGain", ascending=False)
        .reset_index()
        .sort_values("index")
        .index
        + 1
    )
    df["Rank"] = np.where(df.Rank.isin([1, 2, 3, 4, 5]), df.Rank, "Other")

    top_one = get_distrib_returns(df=df[df.Rank == "1"], cf=cf)
    top_one["Order"] = "Top 1"

    top_three = get_distrib_returns(
        df=df[df.Rank.isin(["1", "2", "3"])], cf=cf
    )
    top_three["Order"] = "Top 3"

    top_five = get_distrib_returns(
        df=df[df.Rank.isin(["1", "2", "3", "4", "5"])], cf=cf
    )
    top_five["Order"] = "Top 5"

    others = get_distrib_returns(df=df[df.Rank == "Other"], cf=cf)
    others["Order"] = "Others"

    total = get_distrib_returns(df=df, cf=cf)
    total["Order"] = "Total"

    concentration_rslt = pd.concat(
        [top_one, top_three, top_five, others, total]
    ).set_index("Order")[
        ["GrossIrr", "GrossMultiple", "PctRealizedGain", "PctTotalGain"]
    ]

    if realized_only:
        input_data = {
            "realized_concen": concentration_rslt[
                concentration_rslt.index != "Total"
            ],
            "realized_concen_total": concentration_rslt[
                concentration_rslt.index == "Total"
            ],
        }
    else:
        concentration_rslt["PctRealizedGain"] = None
        input_data = {
            "all_concen": concentration_rslt[
                concentration_rslt.index != "Total"
            ],
            "all_concen_total": concentration_rslt[
                concentration_rslt.index == "Total"
            ],
        }

    return input_data


def _generate_stats_by_group(df, cf, group_col):
    # better way than for loop?
    rslt = pd.DataFrame()
    for i in df[group_col].drop_duplicates().to_list():
        print(i)
        group_df = df[df[group_col] == i]
        group_rslt = get_distrib_returns(
            df=group_df, cf=cf[cf.AssetName.isin(group_df.AssetName)]
        )
        group_rslt[group_col] = i
        rslt = pd.concat([rslt, group_rslt])
    rslt["PctTotalGain"] = rslt.InvestmentGain / rslt.InvestmentGain.sum()
    rslt["PctCapital"] = rslt.EquityInvested / rslt.EquityInvested.sum()
    return rslt


def get_distrib_returns(df, cf):
    # above = df[df.CostType == 'Above Cost']
    # df['PctTotalGain'] = df.InvestmentGain / df.InvestmentGain.sum()
    df["PctCapital"] = df.EquityInvested / df.EquityInvested.sum()
    if len(df) == 0 or len(cf) == 0:
        return pd.DataFrame(
            {
                "TotalValue": [0],
                "EquityInvested": [0],
                "PctCapital": [0],
                "GrossMultiple": [0],
                "GrossIrr": [0],
                "NumInvestments": [0],
            }
        )

    # "Group" should be flexible
    df["Group"] = "All"

    rslt = (
        df[
            [
                "Group",
                "TotalValue",
                "EquityInvested",
                "InvestmentGain",
                "PctCapital",
                "PctRealizedGain",
                "PctTotalGain",
            ]
        ]
        .groupby(["Group"])
        .sum()
    )
    rslt["GrossMultiple"] = calc_multiple(
        cf[cf.AssetName.isin(df.AssetName)], group_cols=None, type="Gross"
    )
    rslt["GrossIrr"] = calc_irr(
        cf[cf.AssetName.isin(df.AssetName)], group_cols=None, type="Gross"
    )
    rslt["NumInvestments"] = len(df.AssetName.unique())

    return rslt


def get_distrib_returns_rpt(df, cf, realized_only):
    # distribution of returns
    if realized_only:
        df = df[df.Status == "Realized"]
    df["PctTotalGain"] = df.InvestmentGain / df.InvestmentGain.sum()
    df["PctCapital"] = df.EquityInvested / df.EquityInvested.sum()

    df = df.assign(
        CostType=lambda v: v.InvestmentGain.apply(
            lambda InvestmentGain: "Below Cost"
            if InvestmentGain < 0
            else "Above Cost"
            if InvestmentGain > 0
            else "At Cost"
            if InvestmentGain == 0
            else "N/A"
        ),
    )
    df = df[df.CostType != "N/A"]
    cost_type_stats = (
        _generate_stats_by_group(df, cf, "CostType")
        .set_index("CostType")
        .reindex(["Above Cost", "At Cost", "Below Cost"])
    )
    total = get_distrib_returns(df=df, cf=cf)

    distribution_rslt = pd.concat([cost_type_stats, total]).reset_index()[
        ["NumInvestments", "GrossMultiple", "GrossIrr", "PctCapital"]
    ]

    if realized_only:
        input_data = {
            "realized_distrib": distribution_rslt[
                distribution_rslt.index != distribution_rslt.index[-1]
            ],
            "realized_distrib_total": distribution_rslt[
                distribution_rslt.index == distribution_rslt.index[-1]
            ],
        }
    else:
        input_data = {
            "all_distrib": distribution_rslt[
                distribution_rslt.index != distribution_rslt.index[-1]
            ],
            "all_distrib_total": distribution_rslt[
                distribution_rslt.index == distribution_rslt.index[-1]
            ],
        }

    return input_data


def get_perf_concentration_rpt(df, cf, realized_only=False):
    # concentration
    df["PctTotalGain"] = df.InvestmentGain / df.InvestmentGain.sum()
    df["PctRealizedGain"] = (
        df.InvestmentGain
        / df[df.Status == "Realized"].InvestmentGain.sum()
    )
    df["PctCapital"] = df.EquityInvested / df.EquityInvested.sum()
    if realized_only:
        df = df[df.Status == "Realized"]
        cf = cf[cf.AssetName.isin(df[df.Status == "Realized"].AssetName)]
        df["PctCapital"] = df.EquityInvested / df.EquityInvested.sum()

    df["Rank"] = (
        df.sort_values("InvestmentGain", ascending=False)
        .reset_index()
        .sort_values("index")
        .index
        + 1
    )
    df["Rank"] = np.where(df.Rank.isin([1, 2, 3, 4, 5]), df.Rank, "Other")
    top_deals = df[df.Rank != "Other"]
    top_deals_multiple = calc_multiple(
        cf[cf.AssetName.isin(top_deals.AssetName)],
        group_cols=["AssetName"],
        type="Gross",
    )
    top_deal_irr = calc_irr(
        cf[cf.AssetName.isin(top_deals.AssetName)],
        group_cols=["AssetName"],
        type="Gross",
    )

    top_deals_rslt = (
        top_deals.sort_values("Rank")
        .merge(
            top_deals_multiple,
            how="left",
            left_on="AssetName",
            right_on="AssetName",
        )
        .merge(
            top_deal_irr,
            how="left",
            left_on="AssetName",
            right_on="AssetName",
        )[
            [
                "AssetName",
                "InvestmentName",
                "InvestmentDate",
                "EquityInvested",
                "ExitDate",
                "UnrealizedValueGross",
                "TotalValue",
                "InvestmentGain",
                "GrossMultiple",
                "GrossIrr",
                "PctRealizedGain",
                "PctTotalGain",
            ]
        ]
    )
    others = df[df.Rank == "Other"]
    if len(others) > 0:
        others = (
            others[
                [
                    "Rank",
                    "EquityInvested",
                    "UnrealizedValueGross",
                    "TotalValue",
                    "InvestmentGain",
                    "PctRealizedGain",
                    "PctTotalGain",
                ]
            ]
            .groupby(["Rank"])
            .sum()
        )
        others["GrossMultiple"] = calc_multiple(
            cf[cf.AssetName.isin(df[df.Rank == "Other"].AssetName)],
            group_cols=None,
            type="Gross",
        )
        others["GrossIrr"] = calc_irr(
            cf[cf.AssetName.isin(df[df.Rank == "Other"].AssetName)],
            group_cols=None,
            type="Gross",
        )
        others["AssetName"] = "Other"
        others_rslt = others[
            [
                "AssetName",
                "EquityInvested",
                "UnrealizedValueGross",
                "TotalValue",
                "InvestmentGain",
                "GrossMultiple",
                "GrossIrr",
                "PctRealizedGain",
                "PctTotalGain",
            ]
        ]
    else:
        others_rslt = pd.DataFrame(
            columns=[
                "AssetName",
                "EquityInvested",
                "UnrealizedValueGross",
                "TotalValue",
                "InvestmentGain",
                "GrossMultiple",
                "GrossIrr",
                "PctRealizedGain",
                "PctTotalGain",
            ]
        )
    df["Group"] = "all"
    total = (
        df[
            [
                "Group",
                "EquityInvested",
                "UnrealizedValueGross",
                "TotalValue",
                "InvestmentGain",
                "PctRealizedGain",
                "PctTotalGain",
            ]
        ]
        .groupby("Group")
        .sum()
    )
    total["GrossMultiple"] = calc_multiple(
        cf[cf.AssetName.isin(df.AssetName)], group_cols=None, type="Gross"
    )
    total["GrossIrr"] = calc_irr(
        cf[cf.AssetName.isin(df.AssetName)], group_cols=None, type="Gross"
    )
    total["AssetName"] = "Total"
    total_rslt = total[
        [
            "AssetName",
            "EquityInvested",
            "UnrealizedValueGross",
            "TotalValue",
            "InvestmentGain",
            "GrossMultiple",
            "GrossIrr",
            "PctRealizedGain",
            "PctTotalGain",
        ]
    ]

    perf_concentration_rslt = pd.concat(
        [top_deals_rslt, others_rslt, total_rslt]
    )[
        [
            "AssetName",
            "InvestmentName",
            "InvestmentDate",
            "EquityInvested",
            # 'ExitDate',
            "UnrealizedValueGross",
            "TotalValue",
            "InvestmentGain",
            "GrossMultiple",
            "GrossIrr",
            "PctRealizedGain",
            "PctTotalGain",
        ]
    ]
    if realized_only:
        top_realized_deals_total = perf_concentration_rslt[
            perf_concentration_rslt.AssetName == "Total"
        ]
        top_realized_deals_total["AssetName"] = str(
            "Total (" + str(len(df.AssetName.unique())) + ")"
        )
        input_data = {
            "top_realized_deals": perf_concentration_rslt[
                perf_concentration_rslt.AssetName != "Total"
            ],
            "top_realized_deals_total": perf_concentration_rslt[
                perf_concentration_rslt.AssetName == "Total"
            ],
        }
    else:
        perf_concentration_rslt["PctRealizedGain"] = None
        top_deals_total = perf_concentration_rslt[
            perf_concentration_rslt.AssetName == "Total"
        ]
        top_deals_total["AssetName"] = (
            "Total (" + str(len(df.AssetName.unique())) + ")"
        )
        input_data = {
            "top_deals": perf_concentration_rslt[
                perf_concentration_rslt.AssetName != "Total"
            ],
            "top_deals_total": top_deals_total,
        }

    return input_data


def get_mgr_rpt_dict(
    manager_attrib: pd.DataFrame,
    fund_attrib: pd.DataFrame,
    deal_attrib: pd.DataFrame,
    deal_cf: pd.DataFrame,
):
    full_manager_tr, full_manager_tr_total = get_mgr_tr_rpt(
        manager_attrib, fund_attrib, deal_attrib, deal_cf
    )
    realized_manager_tr, realized_manager_tr_total = get_mgr_tr_rpt(
        manager_attrib=manager_attrib,
        fund_attrib=fund_attrib,
        deal_attrib=deal_attrib[deal_attrib.Status == "Realized"],
        deal_cf=deal_cf[
            deal_cf.AssetName.isin(
                deal_attrib[deal_attrib.Status == "Realized"].AssetName
            )
        ],
    )
    unrealized_manager_tr, unrealized_manager_tr_total = get_mgr_tr_rpt(
        manager_attrib=manager_attrib,
        fund_attrib=fund_attrib,
        deal_attrib=deal_attrib[deal_attrib.Status == "Unrealized"],
        deal_cf=deal_cf[
            deal_cf.AssetName.isin(
                deal_attrib[deal_attrib.Status == "Unrealized"].AssetName
            )
        ],
    )
    input_data = {
        "full_manager_tr": full_manager_tr,
        "full_manager_tr_total": full_manager_tr_total,
        "realized_manager_tr": realized_manager_tr,
        "realized_manager_tr_total": realized_manager_tr_total,
        "unrealized_manager_tr": unrealized_manager_tr,
        "unrealized_manager_tr_total": unrealized_manager_tr_total,
    }
    return input_data


def get_mgr_tr_rpt(
    manager_attrib: pd.DataFrame,
    fund_attrib: pd.DataFrame,
    deal_attrib: pd.DataFrame,
    deal_cf: pd.DataFrame,
    all_deals=False,
):
    deal_attrib["NumInvestments"] = 1
    summable_df = (
        deal_attrib[
            [
                "InvestmentName",
                "NumInvestments",
                "EquityInvested",
                "RealizedValueGross",
                "UnrealizedValueGross",
                "TotalValue",
            ]
        ]
        .groupby("InvestmentName")
        .sum()
    )
    summable_df.loc["Total", :] = summable_df.sum().values

    gross_multiple = (
        deal_cf[deal_cf.TransactionType.isin(["D", "R"])]
        .groupby("InvestmentName")
        .BaseAmount.sum()
        / deal_cf[deal_cf.TransactionType.isin(["T"])]
        .groupby("InvestmentName")
        .BaseAmount.sum()
        .abs()
    )
    gross_multiple_rslt = pd.concat(
        [
            gross_multiple.reset_index(),
            pd.DataFrame(
                {
                    "InvestmentName": "Total",
                    "BaseAmount": [
                        deal_cf[
                            deal_cf.TransactionType.isin(["D", "R"])
                        ].BaseAmount.sum()
                        / abs(
                            deal_cf[
                                deal_cf.TransactionType.isin(["T"])
                            ].BaseAmount.sum()
                        )
                    ],
                }
            ),
        ]
    ).rename(columns={"BaseAmount": "GrossMultiple"})

    gross_irr = deal_cf.groupby("InvestmentName")[
        ["TransactionDate", "BaseAmount"]
    ].apply(xirr)
    gross_irr_rslt = pd.concat(
        [
            gross_irr.reset_index(),
            pd.DataFrame(
                {
                    "InvestmentName": "Total",
                    0: [
                        xirr(
                            deal_cf[["TransactionDate", "BaseAmount"]]
                            .groupby("TransactionDate")
                            .sum()
                            .reset_index()
                        )
                    ],
                }
            ),
        ]
    ).rename(columns={0: "GrossIrr"})

    deal_attrib["LossRatio"] = np.where(
        deal_attrib.InvestmentGain < 0,
        deal_attrib.InvestmentGain.abs() / deal_attrib.EquityInvested,
        0,
    )
    loss_ratio = (
        abs(
            deal_attrib[deal_attrib.LossRatio != 0]
            .groupby("InvestmentName")
            .InvestmentGain.sum()
        )
        / deal_attrib.groupby("InvestmentName").EquityInvested.sum()
    )
    loss_ratio_rslt = pd.concat(
        [
            loss_ratio.reset_index(),
            pd.DataFrame(
                {
                    "InvestmentName": "Total",
                    0: [
                        abs(
                            deal_attrib[
                                deal_attrib.LossRatio != 0
                            ].InvestmentGain.sum()
                        )
                        / deal_attrib.EquityInvested.sum()
                    ],
                }
            ),
        ]
    ).rename(columns={0: "LossRatio"})

    manager_attrib["InvestmentName"] = "Total"
    reported_values = pd.concat([fund_attrib, manager_attrib])[
        [
            "InvestmentName",
            "VintageYear",
            "CommittedCapital",
            "NetIrr",
            "NetDpi",
            "NetTvpi",
        ]
    ]

    big_merge_on_fund_name = (
        summable_df.merge(
            reported_values,
            how="left",
            left_on="InvestmentName",
            right_on="InvestmentName",
        )
        .merge(
            gross_multiple_rslt,
            how="left",
            left_on="InvestmentName",
            right_on="InvestmentName",
        )
        .merge(
            gross_irr_rslt,
            how="left",
            left_on="InvestmentName",
            right_on="InvestmentName",
        )
        .merge(
            loss_ratio_rslt,
            how="left",
            left_on="InvestmentName",
            right_on="InvestmentName",
        )
    )

    result = big_merge_on_fund_name[
        [
            "InvestmentName",
            "VintageYear",
            "CommittedCapital",
            "NumInvestments",
            "EquityInvested",
            "RealizedValueGross",
            "UnrealizedValueGross",
            "TotalValue",
            "GrossMultiple",
            "GrossIrr",
            "LossRatio",
            "NetTvpi",
            "NetIrr",
            "NetDpi",
        ]
    ]
    # DT: TODO: needed?
    # if not all_deals or self._exclude_net:
    #     result.drop(columns=['NetTvpi', 'NetIrr', 'NetDpi'], inplace=True)

    full_manager_tr = result[result.InvestmentName != "Total"]
    full_manager_tr_total = result[result.InvestmentName == "Total"]
    full_manager_tr_total["InvestmentName"] = (
        "Total (" + str(len(full_manager_tr)) + ")"
    )

    return full_manager_tr, full_manager_tr_total
