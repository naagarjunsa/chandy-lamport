from typing import Dict


class Snapshot:

    class LocalState:
        def __init__(self, balance):
            self.balance = balance

        def __repr__(self):
            return f"Balance : ${self.balance}"

    class ChannelState:
        def __init__(self):
            self.messages = []
            self.marker_received = False

        def __repr__(self):
            return "[" + " | " .join([str(message) for message in self.messages]) + " ] "

    def __init__(self, current_balance, initiator_id, snapshot_id, client_id, client_id_list):
        # record self's local state
        self.client_id = client_id
        self.snapshot_id = snapshot_id
        self.local_state = Snapshot.LocalState(current_balance)
        self.channel_state_dict = dict()
        self.initiator_id = initiator_id
        # tracking the number of markers received
        self.marker_count = 0
        for client_id in client_id_list:
            self.channel_state_dict[client_id] = Snapshot.ChannelState()

    def __repr__(self):
        output_str_list = []
        output_str_list.append(f"Client id : {self.client_id}")
        output_str_list.append(f"Local state :")
        output_str_list.append(f"{self.local_state}")
        output_str_list.append("Channel states :")
        for client_id, channel_state in self.channel_state_dict.items():
            output_str_list.append(f"{client_id} : {channel_state}")
        return "\n".join(output_str_list) + "\n"


class GlobalSnapshot:

    def __init__(self, snapshot_id):
        # dict of type Snapshot
        self.snapshot_id = snapshot_id
        self.snapshot_count = 0
        self.partial_snapshots: Dict[str:Snapshot] = dict()

    def __repr__(self):
        output_str_list = []
        output_str_list.append(f"Snapshot id : {self.snapshot_id}")
        for client_id, partial_snapshot in self.partial_snapshots.items():
            output_str_list.append("=====================================================")
            output_str_list.append(f"{partial_snapshot}")
            output_str_list.append("=====================================================")
        return "\n".join(output_str_list)


