from typing import List
import json


class ActivityParams(object):
    def __init__(self, activity_name, params):
        self.activity_name = activity_name
        self.params = params

    def to_dict(self):
        return {
            "_legacy_activity_name": self.activity_name,
            "_legacy_activity_params": json.dumps(self.params),
        }


class ActivitySet(object):
    def __init__(self, activity_params: List[ActivityParams]):
        self.activity_params = activity_params


class LegacyTasks(object):
    def __init__(self, activity_sets: List[ActivitySet]):
        self.activity_sets = activity_sets
