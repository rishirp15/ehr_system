import time
import threading
from collections import deque

class MaekawaMutex:
    def __init__(self, node_id, quorum_ids):
        self.node_id = node_id
        self.quorum_ids = quorum_ids
        self.state = 'RELEASED'
        self.request_ts = None
        self.outstanding_replies = set()
        self.deferred_requests = deque()
        self.voted = False
        self.first_acquire_attempt = True
        self.lock = threading.Lock()

    def handle_request_rpc(self, requester_id, requester_ts):
        higher_priority = (self.request_ts is not None and \
                          (requester_ts < self.request_ts or \
                          (requester_ts == self.request_ts and requester_id < self.node_id)))

        if self.state == 'HELD' or self.voted or higher_priority:
            self.deferred_requests.append({'id': requester_id, 'ts': requester_ts})
            return 'DEFERRED'
        else:
            self.voted = True
            return 'REPLY'

    def handle_release_rpc(self):
        self.voted = False
        if self.deferred_requests:
            self.deferred_requests = deque(sorted(self.deferred_requests, key=lambda r: (r['ts'], r['id'])))
            next_requester = self.deferred_requests.popleft()
            self.voted = True
            return {'status': 'GRANT_DEFERRED', 'to': next_requester['id']}
        return {'status': 'OK'}

    def receive_reply(self, sender_id):
        if sender_id in self.outstanding_replies:
            self.outstanding_replies.remove(sender_id)
        if not self.outstanding_replies:
            self.state = 'HELD'
            print(f"✅ [Mutex-{self.node_id}] Lock ACQUIRED!")