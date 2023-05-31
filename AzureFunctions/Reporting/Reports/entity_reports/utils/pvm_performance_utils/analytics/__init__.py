import datetime as dt
from typing import List
import pandas as pd
from functools import reduce
import numpy as np
from .standards import (
    get_holding_periods_rpt,
    recurse_down_order,
    get_direct_alpha_rpt,
    get_sum_df_rpt,
    get_ks_pme_rpt,
    get_ror_ctr_df_rpt,
    get_horizon_irr_df_rpt,
    get_horizon_tvpi_df_rpt,
    get_dpi_df_rpt,
    get_hit_rate,
)


def format_performance_report(
    owner: str,
    list_of_rpt_dfs: List[pd.DataFrame],
    list_to_iterate: List[List[str]],
    full_cfs: pd.DataFrame,
    _attributes_needed: List[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # report specific formatting
    attrib = full_cfs[_attributes_needed].drop_duplicates()
    attrib["Portfolio"] = owner

    for group in range(0, len(list_to_iterate)):
        attrib["Group" + str(group)] = attrib.apply(
            lambda x: "_".join(
                [str(x[i]) for i in list_to_iterate[group]]
            ),
            axis=1,
        )
    attrib = attrib.sort_values(
        [
            col_name
            for col_name in reversed(
                attrib.columns[attrib.columns.str.contains("Group")]
            )
        ]
    )

    if "PredominantSector" in attrib.columns:
        attrib.PredominantSector = attrib.PredominantSector.str.replace(
            "FUNDS-", ""
        )
        attrib.PredominantSector = attrib.PredominantSector.str.replace(
            "COS-", ""
        )
    if "PredominantRealizationTypeCategory" in attrib.columns:
        attrib.PredominantRealizationTypeCategory = (
            attrib.PredominantRealizationTypeCategory.str.replace(
                "Realized & Partially/Substantially Realized",
                "Realized & Partial",
            )
        )
        attrib.PredominantRealizationTypeCategory = np.where(
            attrib.PredominantRealizationTypeCategory.isnull(),
            "Not Tagged",
            attrib.PredominantRealizationTypeCategory,
        )

    if "PredominantInvestmentType" in attrib.columns:
        attrib["Order"] = np.select(
            [
                (attrib.PredominantInvestmentType == "Primary Fund"),
                (attrib.PredominantInvestmentType == "Secondary"),
                (
                    attrib.PredominantInvestmentType
                    == "Co-investment/Direct"
                ),
            ],
            [1, 2, 3],
        )
        attrib = attrib.sort_values("Order")
    if "VintageYear" in attrib.columns:
        attrib = attrib.sort_values("VintageYear")
        attrib.VintageYear = attrib.VintageYear.astype(int).astype(str)
    if "PredominantAssetRegion" in attrib.columns:
        attrib = attrib.sort_values("PredominantAssetRegion")

    ordered_recursion = [
        item for sublist in list_to_iterate for item in sublist
    ]
    ordered_recursion = [
        ordered_recursion[i]
        for i in range(len(ordered_recursion))
        if i == ordered_recursion.index(ordered_recursion[i])
    ]
    ordered_rpt_items, counter_df = recurse_down_order(
        attrib, group_by_list=ordered_recursion, depth=0, counter=0
    )
    if "VintageYear" in attrib.columns:
        ordered_rpt_items.Name = ordered_rpt_items.Name.str.replace(
            ".0", ""
        )
        ordered_rpt_items.DisplayName = (
            ordered_rpt_items.DisplayName.str.replace(".0", "")
        )
        for i in list_of_rpt_dfs:
            i.Name = i.Name.str.replace(".0", "")
    rslt = reduce(
        lambda left, right: pd.merge(
            left, right, on=["Name"], how="outer"
        ),
        [ordered_rpt_items] + list_of_rpt_dfs,
    )
    rslt = rslt.sort_values(
        by=["Counter", "Commitment"], ascending=[False, False]
    )
    columns = [
        "DisplayName",
        "MaxNavDate",
        "Duration",
        "Commitment",
        "Nav",
        # "HitRate",
        # "PnlRatio",
        # "SizeRatio",
        "ITD_KsPme",
        "ITD_DirectAlpha",
        # "AlphaHitRate",
        "ITD_GrossMultiple",
        "ITD_GrossIrr",
        "GrossDpi",
        "ITD_AnnRor",
        "ITD_Ctr",
        "5Y_KsPme",
        "5Y_DirectAlpha",
        "5Y_GrossMultiple",
        "5Y_GrossIrr",
        "5Y_AnnRor",
        "5Y_Ctr",
        "3Y_KsPme",
        "3Y_DirectAlpha",
        "3Y_GrossMultiple",
        "3Y_GrossIrr",
        "3Y_AnnRor",
        "3Y_Ctr",
        "TTM_AnnRor",
        "TTM_Ctr",
        "QTD_AnnRor",
        "QTD_Ctr",
    ]
    for col in columns:
        if col not in list(rslt.columns):
            rslt[col] = None
    rslt = rslt[columns]
    rslt = rslt[~rslt.DisplayName.isnull()]

    flat_data = rslt.merge(
        attrib, how="left", left_on="DisplayName", right_on="DealName"
    )
    flat_data["Name"] = np.where(
        flat_data.Name.isnull(), flat_data.DisplayName, flat_data.Name
    )
    return rslt, ordered_rpt_items, flat_data


def get_performance_report_dict(
    owner: str,
    list_to_iterate: List[List[str]],
    full_cfs: pd.DataFrame,
    irr_cfs: pd.DataFrame,
    commitment_df: pd.DataFrame,
    nav_df: pd.DataFrame,
    as_of_date: dt.date,
    _attributes_needed: List[str],
    _trailing_periods: dict,
) -> dict[str, pd.DataFrame]:
    direct_alpha, discount_df = get_direct_alpha_rpt(
        as_of_date=as_of_date,
        df=irr_cfs,
        nav_df=nav_df,
        list_to_iterate=list_to_iterate,
        _attributes_needed=_attributes_needed,
        _trailing_periods=_trailing_periods,
    )
    ks_pme = get_ks_pme_rpt(
        as_of_date=as_of_date,
        df=irr_cfs,
        nav_df=nav_df,
        list_to_iterate=list_to_iterate,
        _attributes_needed=_attributes_needed,
        _trailing_periods=_trailing_periods,
    )
    ror_ctr_df = get_ror_ctr_df_rpt(
        as_of_date=as_of_date,
        df=full_cfs,
        owner=owner,
        list_to_iterate=list_to_iterate,
        _attributes_needed=_attributes_needed,
        _trailing_periods=_trailing_periods,
    )

    horizon_irr = get_horizon_irr_df_rpt(
        as_of_date=as_of_date,
        df=irr_cfs,
        nav_df=nav_df,
        list_to_iterate=list_to_iterate,
        _attributes_needed=_attributes_needed,
        _trailing_periods=_trailing_periods,
    )
    horizon_multiple = get_horizon_tvpi_df_rpt(
        as_of_date=as_of_date,
        df=irr_cfs,
        nav_df=nav_df,
        list_to_iterate=list_to_iterate,
        _attributes_needed=_attributes_needed,
        _trailing_periods=_trailing_periods,
    )
    dpi_rslt = get_dpi_df_rpt(
        df=irr_cfs,
        list_to_iterate=list_to_iterate,
        _attributes_needed=_attributes_needed,
    )

    commitment_df = get_sum_df_rpt(
        commitment_df, list_to_iterate, "Commitment"
    )[["Name", "Commitment"]]
    # TODO: change to ENUM and only D/T/R for IRR cfs
    nav = irr_cfs[irr_cfs.TransactionType == "R"].rename(
        columns={"BaseAmount": "Nav"}
    )
    nav_df = get_sum_df_rpt(nav, list_to_iterate, "Nav")[["Name", "Nav"]]

    discount_df_with_attrib = discount_df[
        ["Name", "Date", "Discounted", "Type"]
    ].merge(
        full_cfs[_attributes_needed + ["Portfolio"]].drop_duplicates(),
        how="left",
        left_on="Name",
        right_on="Name",
    )
    assert len(discount_df) == len(discount_df_with_attrib)

    holding_period_df, max_nav_date = get_holding_periods_rpt(
        irr_cfs,
        discount_df_with_attrib,
        list_to_iterate,
        _attributes_needed,
    )
    pme_hit_rate_df = (
        full_cfs[["Portfolio"] + _attributes_needed]
        .drop_duplicates()
        .merge(
            ks_pme[["Name", "ITD_KsPme"]],
            how="left",
            left_on="Name",
            right_on="Name",
        )
        .merge(commitment_df, how="left", left_on="Name", right_on="Name")
    )
    pme_hit_rate_df["pct_commitment"] = (
        pme_hit_rate_df.Commitment / pme_hit_rate_df.Commitment.sum()
    )
    pme_hit_rate_df = pme_hit_rate_df[~pme_hit_rate_df.ITD_KsPme.isnull()]
    pme_hit_rate_df["HitRateType"] = np.where(
        pme_hit_rate_df.ITD_KsPme > 1, 1, 0
    )

    pme_hit_rate_rslt = get_hit_rate(
        df=pme_hit_rate_df,
        discount_df=discount_df_with_attrib,
        list_to_iterate=list_to_iterate,
    ).rename(
        columns={
            "HitRateType": "HitRate",
            "pct_commitment": "SizeRatio",
            "Discounted": "PnlRatio",
        }
    )

    # report specific formatting
    list_of_rpt_dfs = [
        commitment_df,
        holding_period_df,
        max_nav_date,
        nav_df,
        horizon_irr,
        horizon_multiple,
        ks_pme,
        direct_alpha,
        ror_ctr_df,
        dpi_rslt,
        # alpha_hit_rate_rslt,
        # pme_hit_rate_rslt,
    ]
    (
        formatted_rslt,
        ordered_rpt_items,
        flat_data,
    ) = format_performance_report(
        list_of_rpt_dfs=list_of_rpt_dfs,
        owner=owner,
        list_to_iterate=list_to_iterate,
        full_cfs=full_cfs,
        _attributes_needed=_attributes_needed,
    )
    # 'FormatType' named_range should be Group1,
    # 'FormatSector' named_range should be Group2,
    # 1:n number of groups should be dynamic
    ordered_rpt_items.reset_index(inplace=True, drop=True)
    # formatted_rslt.DisplayName = [str(x)[str(x).find('ticker_')+7:] if 'ticker_' in str(x) else str(x) for x in formatted_rslt.DisplayName]

    # formatted_rslt = formatted_rslt[~formatted_rslt.DisplayName.isin(
    #     ordered_rpt_items[ordered_rpt_items.Layer == ordered_rpt_items.Layer.max()].DisplayName
    # )]
    input_data = {"Data": formatted_rslt, "FlatData": flat_data}

    group_range_map = {1: "FormatType", 2: "FormatSector", 3: "GroupThree"}
    for group_number in ordered_rpt_items.Layer.unique():
        if (
            group_number == ordered_rpt_items.Layer.min()
            or group_number == ordered_rpt_items.Layer.max()
        ):
            continue
        else:
            input_data.update(
                {
                    group_range_map[group_number]: ordered_rpt_items[
                        ordered_rpt_items.Layer == group_number
                    ][["DisplayName"]]
                }
            )

    return input_data
