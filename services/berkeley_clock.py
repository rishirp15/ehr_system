import time
from statistics import mean

class BerkeleyClock:
    def __init__(self, node_id, is_master=False):
        self.node_id = node_id
        self.is_master = is_master
        self.time_offset = 0.0

    def get_time(self):
        return time.time() + self.time_offset

    def get_time_for_sync(self):
        return time.time() + self.time_offset

    def adjust_time(self, adjustment):
        self.time_offset += adjustment
        print(f"[Clock-{self.node_id}] Adjusted time by {adjustment:.4f}s. New offset: {self.time_offset:.4f}s")