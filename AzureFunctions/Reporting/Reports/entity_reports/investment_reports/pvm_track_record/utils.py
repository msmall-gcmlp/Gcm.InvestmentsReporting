import pandas as pd
from gcm.inv.models.pvm.node_evaluation.evaluation_provider.df_utils import (
    ATOMIC_COUNT,
)
from gcm.inv.utils.pvm.node import PvmNodeBase


def enhanced_display_name(inputted_frame: pd.DataFrame) -> pd.DataFrame:
    if ATOMIC_COUNT in inputted_frame.columns:

        def _merge_investment_name(item):
            existing_display_name = item[PvmNodeBase._DISPLAY_NAME]
            cnt = item[ATOMIC_COUNT]
            if type(cnt) == int and cnt > 0:
                return f"{existing_display_name} [{cnt}]"
            return existing_display_name

        inputted_frame[PvmNodeBase._DISPLAY_NAME] = inputted_frame.apply(
            lambda x: _merge_investment_name(x), axis=1
        )
    return inputted_frame
