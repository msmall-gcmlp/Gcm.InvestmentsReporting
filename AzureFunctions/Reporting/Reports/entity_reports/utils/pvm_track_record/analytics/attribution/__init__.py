from ...data_handler.investment_container import InvestmentContainerBase
from typing import List


class PvmTrackRecordAttribution(object):
    def __init__(self, investments: List[InvestmentContainerBase]):
        self.investments = investments
