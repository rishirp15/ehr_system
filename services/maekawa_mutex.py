import time
from collections import deque

class MaekawaMutex:
    def __init__(self, node_id, quorum_ids):
        self.node_id = node_id
        self.quorum_ids = quorum_ids
        
        # State variables
        self.state = 'RELEASED'  # RELEASED, WANTED, HELD
        self.request_ts = None
        self.outstanding_replies = set()
        self.deferred_requests = deque()
        self.voted = False

    def handle_request_rpc(self, requester_id, requester_ts):
        """Processes a lock request from a peer."""
        higher_priority = (self.request_ts is not None and
                           (requester_ts < self.request_ts or
                           (requester_ts == self.request_ts and requester_id < self.node_id)))

        if self.state == 'HELD' or self.voted or higher_priority:
            self.deferred_requests.append({'id': requester_id, 'ts': requester_ts})
            return 'DEFERRED'
        else:
            self.voted = True
            return 'REPLY'

    def handle_release_rpc(self):
        """Processes a release message from a peer who held the lock."""
        self.voted = False
        if self.deferred_requests:
            # Sort deferred requests to grant the one with the highest priority (lowest timestamp)
            self.deferred_requests = deque(sorted(self.deferred_requests, key=lambda r: (r['ts'], r['id'])))
            next_requester = self.deferred_requests.popleft()
            self.voted = True
            return {'status': 'GRANT_DEFERRED', 'to': next_requester['id']}
        return {'status': 'OK'}

    def receive_reply(self, sender_id):
        """Called when this node receives a 'REPLY' for its own request."""
        if sender_id in self.outstanding_replies:
            self.outstanding_replies.remove(sender_id)
        if not self.outstanding_replies:
            self.state = 'HELD'
            print(f"✅ [Mutex-{self.node_id}] Lock ACQUIRED!")