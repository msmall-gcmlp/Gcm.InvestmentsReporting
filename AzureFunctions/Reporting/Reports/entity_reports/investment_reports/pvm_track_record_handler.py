from gcm.inv.utils.misc.table_cache_base import Singleton
import pandas as pd


class TrackRecordHandler(object):
    def __init__(self, manager_name: str):
        self.manager_name = manager_name

    @property
    def manager_hierarchy_structure(self) -> pd.DataFrame:
        __name = "__this_manager_struct"
        _item = getattr(self, __name, None)
        if _item is None:
            pass
        return getattr(self, __name, None)


class TrackRecordManagerProvider(metaclass=Singleton):
    def __init__(self):
        self._cache = {}

    def get_manager_tr_info(self, manager_name) -> TrackRecordHandler:
        if manager_name not in self._cache:
            m = TrackRecordHandler(manager_name)
            self._cache[manager_name] = m
        return self._cache[manager_name]
