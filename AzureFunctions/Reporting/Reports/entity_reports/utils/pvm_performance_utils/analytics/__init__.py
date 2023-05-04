import datetime as dt
from typing import List
import pandas as pd
from .standards import *


def __trailing_periods(as_of_date: dt.date):
    return {
        "QTD": 1,
        "YTD": int(as_of_date.month / 3),
        "TTM": 4,
        "3Y": 12,
        "5Y": 20,
        "Incep": "Incep",
    }


def get_twror_by_industry_rpt(
        owner: str,
        list_to_iterate: List[List[str]],
        full_cfs: pd.DataFrame,
        irr_cfs: pd.DataFrame,
        commitment_df: pd.DataFrame,
        nav_df: pd.DataFrame,
        as_of_date: dt.date,
        _attributes_needed: List[str],
        _trailing_periods: dict
) -> dict[str, pd.DataFrame]:
    direct_alpha, discount_df = get_direct_alpha_rpt(
        as_of_date=as_of_date,
        df=irr_cfs,
        nav_df=nav_df,
        list_to_iterate=list_to_iterate,
        _attributes_needed=_attributes_needed,
        _trailing_periods=_trailing_periods
    )
    ks_pme = get_ks_pme_rpt(
        as_of_date=as_of_date,
        df=irr_cfs,
        nav_df=nav_df,
        list_to_iterate=list_to_iterate,
        _attributes_needed=_attributes_needed,
        _trailing_periods=_trailing_periods
    )
    ror_ctr_df = get_ror_ctr_df_rpt(
        as_of_date=as_of_date,
        df=full_cfs,
        owner=owner,
        list_to_iterate=list_to_iterate,
        _attributes_needed=_attributes_needed,
        _trailing_periods=_trailing_periods
    )

    horizon_irr = get_horizon_irr_df_rpt(
        as_of_date=as_of_date,
        df=irr_cfs,
        nav_df=nav_df,
        list_to_iterate=list_to_iterate,
        _attributes_needed=_attributes_needed,
        _trailing_periods=_trailing_periods
    )
    horizon_multiple = get_horizon_tvpi_df_rpt(
        as_of_date=as_of_date,
        df=irr_cfs,
        nav_df=nav_df,
        list_to_iterate=list_to_iterate,
        _attributes_needed=_attributes_needed,
        _trailing_periods=_trailing_periods
    )

    commitment_df = get_sum_df_rpt(commitment_df, list_to_iterate)[
        ["Name", "Commitment", "NoObs"]
    ]

    nav = irr_cfs[irr_cfs.TransactionType == "Net Asset Value"].rename(
        columns={"BaseAmount": "Nav"}
    )
    nav_df = get_sum_df_rpt(nav, list_to_iterate)[
        ["Name", "Nav", "NoObs"]
    ]

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
        irr_cfs, discount_df_with_attrib, list_to_iterate, _attributes_needed
    )

    ror_ctr_melted = pivot_trailing_period_df(ror_ctr_df)
    ks_pme_melted = pivot_trailing_period_df(ks_pme)
    direct_alpha_melted = pivot_trailing_period_df(direct_alpha)
    irr_melted = pivot_trailing_period_df(
        horizon_irr.rename(columns={"NoObs": "Period"})
    )
    multiple_melted = pivot_trailing_period_df(
        horizon_multiple.rename(columns={"NoObs": "Period"})
    )

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
            for col_name in attrib.columns[
                attrib.columns.str.contains("Group")
            ]
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
    rslt = reduce(
        lambda left, right: pd.merge(
            left, right, on=["Name"], how="outer"
        ),
        [
            ordered_rpt_items,
            commitment_df,
            holding_period_df,
            max_nav_date,
            nav_df,
            irr_melted,
            multiple_melted,
            ks_pme_melted,
            direct_alpha_melted,
            ror_ctr_melted,
        ],
    )
    rslt = rslt.sort_values(
        by=["Counter", "Commitment"], ascending=[False, False]
    )

    # TODO how to reorder multiindex columns?
    columns = [
        "DisplayName",
        "MaxNavDate",
        "Duration",
        "Commitment",
        "Nav",
        ("value", "Incep", "KsPme"),
        ("value", "Incep", "DirectAlpha"),
        ("value", "Incep", "GrossMultiple"),
        ("value", "Incep", "GrossIrr"),
        ("value", "Incep", "AnnRor"),
        ("value", "Incep", "Ctr"),
        ("value", "5Y", "KsPme"),
        ("value", "5Y", "DirectAlpha"),
        ("value", "5Y", "GrossMultiple"),
        ("value", "5Y", "GrossIrr"),
        ("value", "5Y", "AnnRor"),
        ("value", "5Y", "Ctr"),
        ("value", "3Y", "KsPme"),
        ("value", "3Y", "DirectAlpha"),
        ("value", "3Y", "GrossMultiple"),
        ("value", "3Y", "GrossIrr"),
        ("value", "3Y", "AnnRor"),
        ("value", "3Y", "Ctr"),
        ("value", "TTM", "AnnRor"),
        ("value", "TTM", "Ctr"),
        ("value", "QTD", "AnnRor"),
        ("value", "QTD", "Ctr"),
    ]
    for col in columns:
        if col not in list(rslt.columns):
            rslt[col] = None
    rslt = rslt[columns]


    # ordered_rpt_items layers are not dynamic. will fail if not 3 layers in this case
    input_data = {
        "Data": rslt,
        "FormatType": ordered_rpt_items[ordered_rpt_items.Layer == 1][
            ["DisplayName"]
        ].drop_duplicates(),
        "FormatSector": ordered_rpt_items[ordered_rpt_items.Layer == 2][
            ["DisplayName"]
        ].drop_duplicates(),
        "GroupThree": ordered_rpt_items[ordered_rpt_items.Layer == 3][
            ["DisplayName"]
        ].drop_duplicates(),
    }

    return input_data
