import datetime as dt
from typing import List
import pandas as pd
from .standards import get_direct_alpha_rpt


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
    nav_df: pd.DataFrame,
    as_of_date: dt.date,
    _attributes_needed: List[str],
) -> dict[str, pd.DataFrame]:
    trailing_periods = __trailing_periods(as_of_date)
    direct_alpha, discount_df = get_direct_alpha_rpt(
        df=irr_cfs,
        nav_df=nav_df,
        list_to_iterate=list_to_iterate,
        _attributes_needed=_attributes_needed,
    )
    ks_pme = self.get_ks_pme_rpt(
        df=irr_cfs, nav_df=nav_df, list_to_iterate=list_to_iterate
    )

    # TODO: make trailing_periods dynamic on YTD/QTD/TTM etc
    ror_ctr_df = self.get_ror_ctr_df_rpt(
        full_cfs,
        list_to_iterate=list_to_iterate,
        trailing_periods=trailing_periods,
    )

    # irr_data = self.get_irr_df_rpt(cf_irr, list_to_iterate=list_to_iterate)
    # multiple_data = self.get_multiple_df_rpt(cf_irr, list_to_iterate=list_to_iterate)

    horizon_irr = self.get_horizon_irr_df_rpt(
        df=irr_cfs, nav_df=nav_df, list_to_iterate=list_to_iterate
    )
    horizon_multiple = self.get_horizon_tvpi_df_rpt(
        df=irr_cfs, nav_df=nav_df, list_to_iterate=list_to_iterate
    )

    commitment = self.get_commitment(self._report_date, convert_to_usd)
    commitment = self.append_deal_attributes(commitment)
    commitment["Portfolio"] = owner

    if holdings_filter is not None:
        commitment = commitment[
            commitment.ReportingName.isin(holdings_filter)
        ]
    if self._type_filter is not None:
        commitment = commitment[
            commitment.PredominantInvestmentType.isin(self._type_filter)
        ]
    if self._asset_class_filter is not None:
        commitment = commitment[
            commitment.PredominantAssetClass.isin(self._asset_class_filter)
        ]
    # commitment = commitment[commitment.PredominantInvestmentType == 'Primary Fund']

    commitment_df = self.get_sum_df_rpt(commitment, list_to_iterate)[
        ["Name", "Commitment", "NoObs"]
    ]

    nav = cf_irr[cf_irr.TransactionType == "Net Asset Value"].rename(
        columns={"BaseAmount": "Nav"}
    )
    nav_df = self.get_sum_df_rpt(nav, list_to_iterate)[
        ["Name", "Nav", "NoObs"]
    ]

    discount_df_with_attrib = discount_df[
        ["Name", "Date", "Discounted", "Type"]
    ].merge(
        df[self._attributes_needed + ["Portfolio"]].drop_duplicates(),
        how="left",
        left_on="Name",
        right_on="Name",
    )
    assert len(discount_df) == len(discount_df_with_attrib)

    holding_period_df, max_nav_date = self.get_holding_periods_rpt(
        cf_irr, discount_df_with_attrib, list_to_iterate
    )

    ror_ctr_melted = self.pivot_trailing_period_df(ror_ctr_df)
    ks_pme_melted = self.pivot_trailing_period_df(ks_pme)
    direct_alpha_melted = self.pivot_trailing_period_df(direct_alpha)
    irr_melted = self.pivot_trailing_period_df(
        horizon_irr.rename(columns={"NoObs": "Period"})
    )
    multiple_melted = self.pivot_trailing_period_df(
        horizon_multiple.rename(columns={"NoObs": "Period"})
    )

    # report specific formatting
    attrib = df[self._attributes_needed].drop_duplicates()
    attrib["Portfolio"] = self._portfolio_ticker

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
    ordered_rpt_items, counter_df = self.recurse_down_order(
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
    # df_stats_to_format = ordered_rpt_items.merge(commitment_df, how='outer', left_on='Name', right_on='Name').\
    #     merge(holding_period_df, how='outer', left_on='Name', right_on='Name').\
    #     merge(max_nav_date, how='outer', left_on='Name', right_on='Name').\
    #     merge(nav_df, how='outer', left_on='Name', right_on='Name').\
    #     merge(irr_melted, how='outer', left_on='Name', right_on='Name').\
    #     merge(multiple_melted, how='outer', left_on='Name', right_on='Name'). \
    #     merge(ks_pme_melted, how='outer', left_on='Name', right_on='Name'). \
    #     merge(direct_alpha_melted, how='outer', left_on='Name', right_on='Name'). \
    #     merge(ror_ctr_melted, how='outer', left_on='Name', right_on='Name')

    ##### ignore all of the below commented out; replaced with more generic recursion
    # top_level = owner
    # group_order = ['PredominantInvestmentType', 'ClassSector', 'Realization']
    # bottom_level = 'Name'
    #
    # rslt = df_stats_to_format[df_stats_to_format.Name == top_level]
    # first_group = group_order[0]
    # first_group_filters = list(attrib[~attrib[first_group].isnull()][first_group].unique())
    #
    # group_filter = first_group_filters[0]
    # for group_filter in first_group_filters:
    #     group_df = df_stats_to_format[df_stats_to_format.Name == group_filter]
    #     rslt = pd.concat([rslt, group_df])
    #
    #     complete_groups = []
    #     complete_filters = []
    #     next_group = [x for x in group_order if x not in complete_groups + [first_group]][0]
    #     for next_group in [x for x in group_order if x not in complete_groups + [first_group]]:
    #         # complete_groups = []
    #
    #         next_group_filters = list(
    #             attrib[attrib[first_group] == group_filter].sort_values(next_group)[next_group].drop_duplicates())
    #         next_group_filter = [x for x in next_group_filters if x not in complete_filters][0]
    #
    #         group_df = df_stats_to_format[df_stats_to_format.Name.isin([next_group_filter])]
    #         rslt = pd.concat([rslt, group_df])
    #         complete_filters.extend([next_group_filter])
    #
    #         if next_group != group_order[-1]:
    #             complete_groups.extend([next_group])
    #             continue
    #         else:
    #             # complete_groups = complete_groups
    #             bottom_level_items = attrib[attrib[next_group] == next_group_filter][
    #                 bottom_level].drop_duplicates().to_list()
    #             bottom_level_df = df_stats_to_format[
    #                 (df_stats_to_format[bottom_level].isin(bottom_level_items))]
    #             rslt = pd.concat([rslt, bottom_level_df])
    #
    #             # attrib[attrib[first_group] == group_filter].sort_values(next_group)[next_group]
    #            # [x for x in next_group_filters if x not in complete_filters]
    #             # complete_groups.extend([next_group])
    #         #
    #         #
    #         #     attrib_group = attrib[
    #         #         (attrib[first_group] == group_filter) & (attrib[next_group] == next_group_filter)]
    #         #     next_groups = [x for x in group_order if x not in complete_groups]
    #         #     if len(next_groups) == 0:
    #         #         bottom_level_items = attrib_group[
    #         #             bottom_level].drop_duplicates().to_list()
    #         #         bottom_level_df = df_stats_to_format[
    #         #             (df_stats_to_format[bottom_level].isin(bottom_level_items))]
    #         #         rslt = pd.concat([rslt, bottom_level_df])
    #         #
    #         # if next_group != group_order[-1]:
    #         #      for next_group_filter in next_group_filters:
    #         #         group_df = df_stats_to_format[df_stats_to_format.Name.isin([next_group_filter])]
    #         #         rslt = pd.concat([rslt, group_df])
    #         #     else:
    #         #         bottom_level_items = attrib_group[
    #         #             bottom_level].drop_duplicates().to_list()
    #         #         bottom_level_df = df_stats_to_format[
    #         #             (df_stats_to_format[bottom_level].isin(bottom_level_items))]
    #         #         rslt = pd.concat([rslt, bottom_level_df])
    #         #         complete_groups.extend([next_group])
    #         #         attrib_group = attrib[(attrib[first_group] == group_filter) & (attrib[next_group] == next_group_filter)]
    #         #         next_groups = [x for x in group_order if x not in complete_groups]
    #         #         if len(next_groups) == 0:
    #         #             bottom_level_items = attrib_group[
    #         #                 bottom_level].drop_duplicates().to_list()
    #         #             bottom_level_df = df_stats_to_format[
    #         #                 (df_stats_to_format[bottom_level].isin(bottom_level_items))]
    #         #             rslt = pd.concat([rslt, bottom_level_df])
    #         #         else:
    #         #             continue
    #
    # #         if group_num + 1 in group_range:
    # #             next_group = group_order[group_num + 1]
    # #
    # #             for group_filter in list(attrib[attrib[group] == group_filter].sort_values(next_group)[next_group].drop_duplicates()):
    # #                 group_df = df_stats_to_format[df_stats_to_format.Name.isin([group_filter])]
    # #                 rslt = pd.concat([rslt, group_df])
    # #
    # #
    # #         else:
    # #             continue
    # #         for z in attrib[attrib[group1] == i].sort_values(group2)[group2].drop_duplicates().to_list():
    # #             group2_df = df_stats_to_format[df_stats_to_format.Name == z]
    # #             rslt = pd.concat([rslt, group2_df])
    # #
    # #             bottom_level_items = attrib[
    # #                 (attrib[group2] == z) & (attrib[bottom_level] != i)][bottom_level].drop_duplicates().to_list()
    # #             bottom_level_df = df_stats_to_format[
    # #                 (df_stats_to_format[bottom_level].isin(bottom_level_items))]
    # #             rslt = pd.concat([rslt, bottom_level_df])
    # #
    # # # existing new
    # # rslt = df_stats_to_format[df_stats_to_format.Name == top_level]
    # # for i in list(attrib[~attrib[group1].isnull()][group1].unique()):
    # #     group1_df = df_stats_to_format[df_stats_to_format.Name == i]
    # #     rslt = pd.concat([rslt, group1_df])
    # #
    # #     for z in attrib[attrib[group1] == i].sort_values(group2)[group2].drop_duplicates().to_list():
    # #         group2_df = df_stats_to_format[df_stats_to_format.Name == z]
    # #         rslt = pd.concat([rslt, group2_df])
    # #
    # #         bottom_level_items = attrib[
    # #             (attrib[group2] == z) & (attrib[bottom_level] != i)][bottom_level].drop_duplicates().to_list()
    # #         bottom_level_df = df_stats_to_format[
    # #             (df_stats_to_format[bottom_level].isin(bottom_level_items))]
    # #         rslt = pd.concat([rslt, bottom_level_df])
    #
    # # old
    # # rslt = df_stats_to_format[df_stats_to_format.Name == owner]
    # # for i in ['Primary Fund', 'Secondary', 'Co-investment/Direct']:
    # #     strat = df_stats_to_format[df_stats_to_format.Name == i]
    # #     rslt = pd.concat([rslt, strat])
    # #     for z in attrib[attrib.PredominantInvestmentType == i].sort_values('ClassSector').ClassSector.drop_duplicates().to_list():
    # #         class_sector = df_stats_to_format[df_stats_to_format.Name == z]
    # #         rslt = pd.concat([rslt, class_sector])
    # #
    # #         investments = attrib[attrib.ClassSector == z].Name.drop_duplicates().to_list()
    # #         investment_stats = df_stats_to_format[df_stats_to_format.Name.isin(investments)].sort_values('Commitment', ascending=False)
    # #         rslt = pd.concat([rslt, investment_stats])
    # attrib_tmp = pd.DataFrame({
    #     'Portfolio': ['Coned'] * 4,
    #     'Region': ['USA', 'EUR'] *2 ,
    #     'Sector': ['TMT', 'Energy', 'Energy', 'Industrials'],
    #     'Deal': ['A', 'B', 'C', 'D'],
    #     'Nav': [20, 13, 50, 12] * 4
    # })
    #
    # report_rslt = pd.DataFrame({
    #     'Name': ['Coned', 'USA', 'EUR', 'TMT', 'Energy', 'Industrials', 'A', 'B', 'C', 'D',
    #              'USA_TMT', 'EUR_Energy', 'USA_Energy', 'EUR_Industials'],
    #     'Nav': [100, 50, 50, 100/4, (100/4)*2, 100/4, 25, 25, 25, 25,
    #             25, 25, 25, 25]
    # })

    # rslt = df_stats_to_format.reset_index(drop=True).drop(columns='NoObs')
    # attrib.PredominantSector = attrib.PredominantSector.str.replace('FUNDS-', '')
    # attrib.PredominantSector = attrib.PredominantSector.str.replace('COS-', '')
    # rslt = rslt.merge(attrib[['PredominantSector', 'ClassSector']].drop_duplicates(),
    #                  how='left', left_on='Name', right_on='ClassSector')
    # rslt.Name = np.where(~rslt.PredominantSector.isnull(), rslt.PredominantSector, rslt.Name)
    # rslt.drop(columns=['PredominantSector', 'ClassSector'], inplace=True)

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
