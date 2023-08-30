import pandas as pd
from ....base_render import BaseRenderer


class OneThreeFive_Bucketed_And_Total(BaseRenderer):
    def __init__(
        self,
        Breakout_1_3_5_Bucketed_And_Other: pd.DataFrame,
        Breakout_1_3_5_Total: pd.DataFrame,
    ):
        self.Breakout_1_3_5_Bucketed_And_Other = (
            Breakout_1_3_5_Bucketed_And_Other
        )
        self.Breakout_1_3_5_Total = Breakout_1_3_5_Total
