from gcm.inv.utils.DaoUtils.query_utils import (
    Query,
    DeclarativeMeta,
    filter_many,
)
from typing import List
import pandas as pd
from gcm.Dao.DaoRunner import DaoRunner, DaoSource
from gcm.inv.scenario import Scenario


def remove_sys_cols(df: pd.DataFrame) -> pd.DataFrame:
    column_to_keep = [str(x) for x in df.columns]
    column_to_keep = [
        x for x in column_to_keep if (not x.upper().startswith("SYSSTART"))
    ]
    column_to_keep = [
        x for x in column_to_keep if (not x.upper().startswith("SYSEND"))
    ]
    return df[column_to_keep]


def get_pivoted_extensions(ids: List[int], type="Investment"):
    dao: DaoRunner = Scenario.get_attribute("dao")

    def oper(query: Query, item: dict[str, DeclarativeMeta]):
        query = filter_many(query, item, f"{type}Id", ids)
        return query

    extensions = remove_sys_cols(
        dao.execute(
            params={
                "table": f"{type}DimnExtension",
                "schema": "PvmTrackRecord",
                "operation": oper,
            },
            source=DaoSource.InvestmentsDwh,
            operation=lambda d, p: d.get_data(p),
        )
    )
    values = [
        "MeasureValueDate",
        "MeasureValueInt",
        "MeasureValueString",
        "MeasureValueFloat",
    ]
    extensions = extensions.pivot(
        columns=["MeasureName"],
        index=[f"{type}Id"],
        values=values,
    ).reset_index()
    simplified_df = pd.DataFrame()
    for c in extensions.columns:
        this_values = extensions[c]
        if any([(not pd.isna(x)) for x in this_values]):
            if c[0] in values:
                simplified_df[c[1]] = this_values
            elif c[1] == "" and c[0] != "":
                simplified_df[c[0]] = this_values

    return simplified_df


def get_dimns(ids: List[int], type="Investment"):
    dao: DaoRunner = Scenario.get_attribute("dao")

    def oper(query: Query, item: dict[str, DeclarativeMeta]):
        query = filter_many(query, item, f"{type}Id", ids)
        return query

    df: pd.DataFrame = remove_sys_cols(
        dao.execute(
            params={
                "table": f"{type}Dimn",
                "schema": "PvmTrackRecord",
                "operation": oper,
            },
            source=DaoSource.InvestmentsDwh,
            operation=lambda d, p: d.get_data(p),
        )
    )

    extens = get_pivoted_extensions(ids, type)
    # remove unnamed
    extens = extens[
        [
            x
            for x in extens.columns
            if (not x.upper().startswith("UNNAMED: "))
        ]
    ]
    df = pd.merge(df, extens, on=[f"{type}Id"])
    return df
