import pickle
import selectors
import socket
import logging
import json
import sys
import pathlib
import os
import time
import threading
import types
import re
from queue import Queue
import enum
from typing import Dict

from snapshot import Snapshot, GlobalSnapshot

logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger(__name__)


# creating enumerations for various Events
class Event(enum.Enum):
    INIT_SNAPSHOT = 1
    MARKER = 2
    TRANSACTION = 3
    PARTIAL_SNAPSHOT = 4
    BROADCAST_MARKER = 5
    UPDATE_CHANNEL_STATE = 6
    EOM = 7


class Client:
    INIT_BALANCE = 10
    EOM_IN_BYTES = pickle.dumps(Event.EOM)

    def __init__(self, client_id, clients_info):
        # parameter vars
        self.client_id = client_id
        self.clients_info = clients_info
        self.incoming_client_id_list = self.get_incoming_client_id_list(client_id, clients_info)
        # stores conn objs of all peers
        self.peer_conn_info = dict()

        # global vars
        self.transaction_thread_queue = Queue()
        self.snapshot_thread_queue = Queue()
        self.balance_lock = threading.Lock()
        self.server_flag = threading.Event()
        self.server_flag.set()
        self.balance = Client.INIT_BALANCE
        self.partial_snapshots_dict: Dict[str: Snapshot] = dict()
        self.global_snapshots_dict: Dict[str: GlobalSnapshot] = dict()
        self.snapshot_id = 0

        # start the client's peer server
        self.server_thread = threading.Thread(target=self.start_server_thread)
        self.server_thread.daemon = True
        self.server_thread.start()

        # connect to all the other clients
        time.sleep(15)
        self.connect_to_other_peers()

        # start the transaction handling thread -> only app level work
        transaction_thread = threading.Thread(target=self.start_transaction_thread)
        transaction_thread.daemon = True
        transaction_thread.start()

        # handles the snapshot work
        snapshot_thread = threading.Thread(target=self.start_snapshot_thread)
        snapshot_thread.daemon = True
        snapshot_thread.start()

        time.sleep(5)
        self.start_ui()

    def __del__(self):
        for _, conn in self.peer_conn_info.items():
            conn.close()

    @staticmethod
    def display_menu():
        logger.info("- Press 1 to make a new transaction.")
        logger.info("- Press 2 to get balance")
        logger.info("- Press 3 to initiate snapshot")
        logger.info("- Press 4 to terminate client")

    @staticmethod
    def accept_wrapper(sock, selector):
        conn, addr = sock.accept()  # Should be ready to read
        logger.info(f'accepted connection from : {addr}')
        conn.setblocking(False)
        data = types.SimpleNamespace(addr=addr, inb=b'', outb=b'')
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        selector.register(conn, events, data=data)

    @staticmethod
    def get_request_list(data):
        byte_message_list = data.split(Client.EOM_IN_BYTES)
        req_list = []
        for msg in byte_message_list:
            if not msg:
                continue
            req = pickle.loads(msg)
            req_list.append(req)
        return req_list

    def service_connection(self, key, mask, selector):
        sock = key.fileobj
        data = key.data
        if mask & selectors.EVENT_READ:
            recv_data = sock.recv(1024)
            if recv_data:
                req_list = self.get_request_list(recv_data)
                for req in req_list:
                    self.handle_request_from_peer(req)
            else:
                print('closing connection to : ', data.addr)
                selector.unregister(sock)
                sock.close()

    def handle_transaction(self, event):
        # can add checks if the key exists but bleh
        receiver_id = event["receiver_id"]
        amount = event["amount"]

        # check if valid request or connected to the graph
        if receiver_id not in self.clients_info:
            logger.error("Client id does not exist. Please try again with a valid client id..")
            return
        elif receiver_id not in self.clients_info[self.client_id]["connected_to"]:
            logger.warning("NOT CONNECTED")
            return

        # check if update the balance and send only if sufficient balance
        send_request_flag = True
        self.balance_lock.acquire()
        if amount > self.balance:
            logger.info("INSUFFICIENT BALANCE")
            send_request_flag = False
        else:
            self.balance -= amount
        self.balance_lock.release()

        # send request to receiver id peer if enough balance
        if send_request_flag:
            request = {"type": Event.TRANSACTION, "sender_id": self.client_id, "amount": amount}
            self.send_transaction_request(receiver_id, request)

    def send_transaction_request(self, receiver_id, request):
        # time to sleep; cause life
        time.sleep(3)

        self.peer_conn_info[receiver_id].sendall(pickle.dumps(request) + pickle.dumps(Event.EOM))
        logger.info(f"Transaction message sent to client {receiver_id} : " + str(request))


    def handle_balance(self):
        self.balance_lock.acquire()
        logger.info(f"Current Balance is : ${self.balance}")
        self.balance_lock.release()

    def start_transaction_thread(self):

        while True:
            if not self.transaction_thread_queue.empty():
                event = self.transaction_thread_queue.get()
                if event["type"] == "transaction":
                    self.handle_transaction(event)
                elif event["type"] == "balance":
                    self.handle_balance()
                elif event["type"] == "quit":
                    break
                else:
                    logger.info("Invalid event to transaction thread")

    def init_snapshot(self):

        # update the counter of the snapshot; no need of lock as the only place of usage
        self.snapshot_id += 1
        curr_snapshot_id = self.client_id + "_" + str(self.snapshot_id)

        # create an entry in the snapshot dict with this curr_snapshot_id as key
        # lock needed as server thread also accesses this map

        self.global_snapshots_dict[curr_snapshot_id] = GlobalSnapshot(curr_snapshot_id)
        self.partial_snapshots_dict[curr_snapshot_id] = Snapshot(self.balance, self.client_id, curr_snapshot_id,
                                                                 self.client_id, self.clients_info.keys())

        # need to record all incoming messages in server side and add to self.client_id channel state
        # taken care of in this method -> {update_channel_state_for_all_snapshots}

        # send markers to all peers
        self.broadcast_marker_request(snapshot_id=curr_snapshot_id, initiator_id=self.client_id)

    def broadcast_marker_request(self, snapshot_id, initiator_id):

        # creating request dict
        request = {"type": Event.MARKER,
                   "snapshot_id": snapshot_id,
                   "initiator_id": initiator_id,
                   "sender_id": self.client_id}

        for peer_id in self.clients_info[self.client_id]["connected_to"]:
            conn = self.peer_conn_info[peer_id]
            time.sleep(3)
            # TODO: Why  is lock needed here ?
            # self.lock.acquire()
            conn.sendall(pickle.dumps(request) + pickle.dumps(Event.EOM))
            logger.info(f"Marker sent to client {peer_id} : " + str(request))
            # self.lock.release()

    def update_channel_state_for_all_snapshots(self, req):

        sender_id = req["sender_id"]
        for snapshot_id, partial_snapshot in self.partial_snapshots_dict.items():
            if not partial_snapshot.channel_state_dict[sender_id].marker_received:
                partial_snapshot.channel_state_dict[sender_id].messages.append(req)
                logger.debug(f"State updated for snapshot_id : {snapshot_id} , channel_id : {sender_id}")

    def start_snapshot_thread(self):
        while True:
            if not self.snapshot_thread_queue.empty():
                event = self.snapshot_thread_queue.get()
                if event["type"] == Event.INIT_SNAPSHOT:
                    self.init_snapshot()
                elif event["type"] == Event.MARKER:
                    self.handle_marker(event["request"])
                elif event["type"] == Event.UPDATE_CHANNEL_STATE:
                    self.update_channel_state_for_all_snapshots(event["request"])
                elif event["type"] == Event.PARTIAL_SNAPSHOT:
                    self.handle_partial_snapshot(event["request"])
                elif event["type"] == "quit":
                    break
                else:
                    logger.info("Invalid event to snapshot thread")

    def start_server_thread(self):
        server_host = self.clients_info[self.client_id]["server_host"]
        server_port = self.clients_info[self.client_id]["server_port"]
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        selector = selectors.DefaultSelector()
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((server_host, server_port))

        server_socket.listen()
        logger.info("Peer Server is up and running!")
        logger.info("Waiting on new connections...")
        server_socket.setblocking(False)
        selector.register(server_socket, selectors.EVENT_READ, data=None)

        while True and self.server_flag.is_set():
            events = selector.select(timeout=None)
            for key, mask in events:
                if key.data is None:
                    self.accept_wrapper(key.fileobj, selector)
                else:
                    self.service_connection(key, mask, selector)

    def start_ui(self):
        user_req_id = 0
        while True:
            user_req_id += 1
            self.display_menu()
            user_input = input("Client prompt >> ").strip()
            if user_input == "1":
                receiver_id = input("Enter receiver client id  >> ").strip()
                amount = float(input("Enter the amount in $$ to be transferred to the above client  >> ").strip())
                # tell the transaction thread by adding the event into transaction queue
                self.transaction_thread_queue.put(
                    {"type": "transaction", "receiver_id": receiver_id, "amount": amount, "user_req_id": user_req_id})
            elif user_input == "2":
                # tell the transaction thread by adding the event into the transaction queue
                self.transaction_thread_queue.put({"type": "balance"})
            elif user_input == "3":
                # tell the snapshot thread by adding the event into the snapshot queue
                self.snapshot_thread_queue.put({"type": Event.INIT_SNAPSHOT})
            elif user_input == "4":
                self.transaction_thread_queue.put({"type": "quit"})
                self.snapshot_thread_queue.put({"type": "quit"})
                self.server_flag.clear()
                logger.info("Until next time...")
                break
            else:
                logger.warning("Incorrect menu option. Please try again..")
                continue

    def connect_to_other_peers(self):
        # connect to all other peers
        for other_client_id, client_info in self.clients_info.items():
            peer_addr = (client_info["server_host"], client_info["server_port"])
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            logger.info("Trying to connect to peer : " + other_client_id)
            sock.connect(peer_addr)
            logger.info("Now connected to peer : " + other_client_id)
            self.peer_conn_info[other_client_id] = sock

    def handle_request_from_peer(self, req):

        sender_id = req["sender_id"]

        logger.info(f"Request received from client {sender_id} : {req}")
        if req["type"] == Event.TRANSACTION:
            amount = req["amount"]
            # process the message this will happen irrespective of snapshot
            self.balance_lock.acquire()
            self.balance += amount
            logger.info(f"Balance updated to : ${self.balance}")
            self.balance_lock.release()
            self.snapshot_thread_queue.put({"type": Event.UPDATE_CHANNEL_STATE, "request": req})
            # logic to store the messages in partial snapshots based on marker values in the snapshot
        elif req["type"] == Event.MARKER:
            self.snapshot_thread_queue.put({"type": Event.MARKER, "request": req})
            self.snapshot_thread_queue.put({"type": Event.UPDATE_CHANNEL_STATE, "request": req})
        elif req["type"] == Event.PARTIAL_SNAPSHOT:
            self.snapshot_thread_queue.put({"type": Event.PARTIAL_SNAPSHOT, "request": req})
        else:
            logger.info(f"Invalid request type to the server {req}")

    def handle_marker(self, req):

        snapshot_id = req["snapshot_id"]
        sender_id = req["sender_id"]
        initiator_id = req["initiator_id"]

        # TODO : handle locking
        # lock not needed as all the snapshot stuff is handled by this thread alone

        # check if the partial map has entry of that snapshot_id and then create new one, case with the first marker
        if snapshot_id not in self.partial_snapshots_dict:
            logger.debug(f"creating new snapshot for snapshot id : {snapshot_id}")
            # 2.3.1.2 -> record local state
            self.partial_snapshots_dict[snapshot_id] = Snapshot(self.balance, initiator_id, snapshot_id, self.client_id,
                                                                self.clients_info.keys())
            # 2.3.1.3 -> send marker to all other channels
            self.broadcast_marker_request(snapshot_id=snapshot_id, initiator_id=initiator_id)

        cur_partial_snapshot: Snapshot = self.partial_snapshots_dict[snapshot_id]
        cur_partial_snapshot.channel_state_dict[sender_id].marker_received = True
        cur_partial_snapshot.marker_count += 1

        # 2.4.1 -> if all markers are recvd for that snapshot_id
        if cur_partial_snapshot.marker_count == len(self.incoming_client_id_list):
            self.send_partial_snapshot(initiator_id, cur_partial_snapshot)

    def send_partial_snapshot(self, initiator_id, partial_snapshot):

        # send message to the initiator of this partial snapshot and delete it lol
        request = {"type": Event.PARTIAL_SNAPSHOT,
                   "partial_snapshot": partial_snapshot,
                   "sender_id": self.client_id,
                   }

        # time to sleep; cause life
        time.sleep(3)

        self.peer_conn_info[initiator_id].sendall(pickle.dumps(request) + pickle.dumps(Event.EOM))
        logger.info(f"Partial snapshot sent to client {initiator_id} : " + str(request))
        # Delete the partial snapshot once sent to the initiator.
        # let the garbage collector take care of the original object
        del self.partial_snapshots_dict[request["partial_snapshot"].snapshot_id]

    def handle_partial_snapshot(self, request):
        partial_snapshot: Snapshot = request["partial_snapshot"]
        sender_id = request["sender_id"]
        cur_global_snapshot = self.global_snapshots_dict[partial_snapshot.snapshot_id]
        cur_global_snapshot.partial_snapshots[sender_id] = partial_snapshot
        cur_global_snapshot.snapshot_count += 1
        if cur_global_snapshot.snapshot_count == len(clients_info):
            self.display_global_snapshot(cur_global_snapshot)

    @staticmethod
    def display_global_snapshot(cur_global_snapshot: GlobalSnapshot):
        logger.info(f"Here's the global snapshot for snapshot id : {cur_global_snapshot.snapshot_id}")
        logger.info(cur_global_snapshot)

    @staticmethod
    def get_incoming_client_id_list(client_id, clients_info):
        incoming_client_id_list = []
        for other_client_id, other_client_info in clients_info.items():
            if client_id in other_client_info["connected_to"]:
                incoming_client_id_list.append(other_client_id)
        logger.debug(f"Incoming client id list : {str(incoming_client_id_list)}")
        return incoming_client_id_list


if __name__ == '__main__':
    client_id = sys.argv[1]
    with open(os.path.join(pathlib.Path(__file__).parent.resolve(), 'config.json'), 'r') as config_file:
        config_info = json.load(config_file)
        clients_info = config_info["clients"]
        if client_id not in clients_info:
            logger.error("Invalid client id. Please check...")
        else:
            logger.info("Initiating client..")
            Client(client_id, clients_info)
