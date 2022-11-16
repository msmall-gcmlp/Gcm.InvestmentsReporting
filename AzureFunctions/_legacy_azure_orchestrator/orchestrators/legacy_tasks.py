from typing import List


class ActivityParams(object):
    def __init__(self, activity_name, params):
        self.activity_name = activity_name
        self.params = params


class ActivitySet(object):
    def __init__(self, activity_params: List[ActivityParams]):
        self.activity_params = activity_params


class LegacyTasks(object):
    def __init__(self, activity_sets: List[ActivitySet]):
        self.activity_sets = activity_sets
