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

logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.DEBUG)

logger = logging.getLogger(__name__)


class Client:

    INIT_BALANCE = 10
    
    def __init__(self, client_id, clients_info):
        #parameter vars
        self.client_id = client_id
        self.clients_info = clients_info
        
        #stores conn objs of all peers
        self.peer_conn_info = dict()

        #global vars
        self.transaction_thread_queue = Queue()
        self.snapshot_thread_queue = Queue()
        self.lock = threading.Lock()
        self.event = threading.Event()
        self.balance = Client.INIT_BALANCE
        self.snapshots = dict()
        self.snapshot_id = 0
        
        #start the client's peer server 
        self.server_thread = threading.Thread(target=self.start_server_thread)
        self.server_thread.daemon = True
        self.server_thread.start()

        #connect to all the other clients
        time.sleep(5)
        self.connect_to_other_peers()

        #start the transaction handling thread -> only app level work
        transaction_thread = threading.Thread(target=self.start_transaction_thread)
        transaction_thread.daemon = True
        transaction_thread.start()

        #handles the snapshot work
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
        print("- Press 1 to make a new transaction.")
        print("- Press 2 to get balance")
        print("- Press 3 to initiate snapshot")
        print("- Press 4 to terminate client")

    def accept_wrapper(self, sock, selector):
        conn, addr = sock.accept()  # Should be ready to read
        logger.info(f'accepted connection from : {addr}')
        conn.setblocking(False)
        data = types.SimpleNamespace(addr=addr, inb=b'', outb=b'')
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        selector.register(conn, events, data=data)

    def get_request_list(self, data):
        start_ids = [m.start() for m in re.finditer('{"type"', data)]
        req_list = list()
        for i in range(1, len(start_ids)):
            req_list.append(data[start_ids[i-1]:start_ids[i]])
        req_list.append(data[start_ids[len(start_ids)-1]:])
        return req_list
    
    def service_connection(self, key, mask, selector):
        sock = key.fileobj
        data = key.data
        client_host, client_port = sock.getpeername()
        if mask & selectors.EVENT_READ:
            recv_data = sock.recv(1024).decode()
            if recv_data:
                req_list = self.get_request_list(recv_data)
                for req in req_list:
                    self.handle_message_from_peer(req)
            else:
                print('closing connection to : ', data.addr)
                selector.unregister(sock)
                sock.close()

    def handle_transaction(self, event):
        #can add checks if the key exists but bleh
        receiver_id = event["receiver_id"]
        amount = event["amount"]

        #check if valid request or connected to the graph 
        if receiver_id not in self.clients_info:
            logger.error("Client id does not exist. Please try again with a valid client id..")
            return
        elif receiver_id not in self.clients_info[self.client_id]["connected_to"]:
            logger.info("NOT CONNECTED")
            return
        
        #check if update the balance and send only if sufficient balance
        send_request_flag = True 
        self.lock.acquire()
        if amount > self.balance:
            logger.info("INSUFFICIENT BALANCE")
            send_request_flag = False
        else:
            self.balance -= amount
        self.lock.release()

        #send request to reciever id peer if enough balance
        if send_request_flag:
            request = {"type": "transaction", "sender_id": self.client_id, "amount": amount}
            self.send_transaction_request(receiver_id, request)
    
    def send_transaction_request(self, receiver_id, request):
        #time to sleep; cause life
        time.sleep(3)
        
        self.lock.acquire()
        self.peer_conn_info[receiver_id].sendall(json.dumps(request).encode())
        logger.info(f"Transaction message sent to client {receiver_id} : " + str(request))
        self.lock.release()
    
    def handle_balance(self):
        self.lock.acquire()
        print(f"Current Balance is {self.balance}")
        self.lock.release()

    def start_transaction_thread(self):

        while True:
            if not self.transaction_thread_queue.empty():
                event = self.transaction_thread_queue.get()

                if event["type"] == "transaction":       
                    self.handle_transaction(event)
                elif event["type"] == "balance":
                    self.handle_balance()
                else:
                    logger.info("Invalid event to transaction thread")

    def init_snapshot(self):

        #update the counter of the snapshot; no need of lock as the only place of usage
        self.snapshot_id += 1
        curr_snapshot_id = self.client_id + str(self.snapshot_id)

        #create an entry in the final snapshot dict with this curr_snapshot_id as key
        #lock needed as server thread also accesses this map
        self.lock.acquire()
        self.snapshots[curr_snapshot_id] = dict()
        
        for client_id in self.clients_info:
            self.snapshots[curr_snapshot_id][client_id] = dict()
            self.snapshots[curr_snapshot_id][client_id]["local_state"] = None
            self.snapshots[curr_snapshot_id][client_id]["channel_state"] = dict()
            self.snapshots[curr_snapshot_id][client_id]["marker_recvd"] = False
            self.snapshots[curr_snapshot_id][client_id]["snapshot_recvd"] = False

        #tracking the number of markers recvd
        self.snapshots[curr_snapshot_id]["marker_count"] = 0
        self.snapshots[curr_snapshot_id]["snapshot_count"] = 0
        #record self's local state
        self.snapshots[curr_snapshot_id][self.client_id]["local_state"] = self.balance

        #need to record all incoming messages in server side and add to self.client_id channel state
        #taken care of in this method -> {add method name here}
        self.lock.release()

        #creating request dict
        request = { "type": "marker",
                    "snapshot_id": curr_snapshot_id, 
                    "init_id": self.client_id, 
                    "sender_id": self.client_id}
        
        #send markers to all peers
        self.send_snapshot_request(request)

    def send_snapshot_request(self, request):
        
        for peer_id, conn in self.peer_conn_info.items():
            time.sleep(3)
            self.lock.acquire()
            conn.sendall(json.dumps(request).encode())
            logger.info(f"Marker sent to client {peer_id} : " + str(request))
            self.lock.release()


    def start_snapshot_thread(self):

        while True:
            if not self.snapshot_thread_queue.empty():
                event = self.snapshot_thread_queue.get()

                if event["type"] == "init_snapshot":
                    self.init_snapshot()
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
        
        while True:
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
                amount = int(input("Enter the amount in $$ to be transferred to the above client  >> ").strip())
                # tell the transaction thread by adding the event into transaction queue
                self.transaction_thread_queue.put({"type": "transaction", "receiver_id": receiver_id, "amount": amount, "user_req_id": user_req_id})
            elif user_input == "2":
                #tell the transaction thread by adding the event into the transaction queue
                self.transaction_thread_queue.put({"type": "balance"})
            elif user_input == "3":
                #tell the snapshot thread by adding the event into the snapshot queue
                self.snapshot_thread_queue.put({"type": "init_snapshot"})
            elif user_input == "4":
                self.handle_quit(client_socket)
                break
            else:
                logger.warning("Incorrect menu option. Please try again..")
                continue

    def connect_to_other_peers(self):
        # connect to all other peers
        for other_client_id, client_info in self.clients_info.items():
            if other_client_id == self.client_id:
                continue

            peer_addr = (client_info["server_host"], client_info["server_port"])
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            logger.info("Trying to connect to peer : " + other_client_id)
            sock.connect(peer_addr)
            logger.info("Now connected to peer : " + other_client_id)
            self.peer_conn_info[other_client_id] = sock

    def handle_message_from_peer(self, msg):
        req = json.loads(msg)
        sender_id = req["sender_id"]
        amount = req["amount"]
        logger.info(f"Message received from client {sender_id} : {msg}")
        if req["type"] == "transaction":

            #process the message this will happen irrespective of snapshot
            self.lock.acquire()
            self.balance += amount
            self.lock.release()

            #logic to store the messages in partial snapshots based on marker values in the snapshot

        elif req["type"] == "marker":
            self.handle_marker(req)
        elif req["type"] == "partial_snapshot":
            self.handle_partial_snapshot(req)
        else:
            logger.info(f"Invalid request type to the server {req}")

    def handle_marker(self, req):

        snapshot_id = req["snapshot_id"]
        sender_id = req["sender_id"]
        init_id = req["init_id"]

        #acquire lock cause working with global vars
        self.lock.acquire()

        #check if the partial map has entry of that snapshot_id and then create new one or else
        if snapshot_id not in self.partial_snapshots:
            self.partial_snapshots[snapshot_id] = dict()
            self.partial_snapshots[snapshot_id]["channel_state"] = dict()
            self.partial_snapshots[snapshot_id]["marker_recvd"] = dict()
            
            #2.3.1.1 -> mark that channel as empty
            for client_id in self.clients_info:
                self.partial_snapshots[snapshot_id]["channel_state"][client_id] = list()
                self.partial_snapshots[snapshot_id]["marker_recvd"][client_id] = False
            
            self.partial_snapshots[snapshot_id]["marker_recvd"][sender_id] = True
            self.partial_snapshots[snapshot_id]["marker_count"] = 1
            
            #2.3.1.2 -> record local state
            self.partial_snapshots[snapshot_id]["local_state"] = self.balance

            #2.3.1.3 -> send marker to all other channels
            request = { "type": "marker", "init_id": init_id,
                        "snapshot_id": snapshot_id, "sender_id": self.client_id}
            

        else:
            #if it is not the first marker for that snapshot_id
            self.partial_snapshots[snapshot_id]["marker_recvd"][sender_id] = True
            self.partial_snapshots[snapshot_id]["marker_count"] += 1

            #2.4.1 -> if all markers are recvd for that snapshot_id 
            if self.partial_snapshots[snapshot_id]["marker_count"] == len(self.clients_info)-1:
                #send message to the initiator of this partial snapshot and delete it lol
                request = { "type": "partial_snapshot", "init_id": init_id, "snapshot_id": snapshot_id,
                            "sender_id": self.client_id,
                            "local_state": self.partial_snapshots[snapshot_id]["local_state"],
                            "channel_state": self.partial_snapshots[snapshot_id]["channel_state"]}
                
                self.send_partial_snapshot(init_id, req)
        self.lock.release()

    def send_partial_snapshot(self, init_id, request):
        #time to sleep; cause life
        time.sleep(3)
        
        self.lock.acquire()
        self.peer_conn_info[init_id].sendall(json.dumps(request).encode())
        logger.info(f"Partial snapshot sent to client {init_id} : " + str(request))
        self.lock.release()

    def handle_quit(self, client_socket):
        # update my clock before sending a message to the server.
        self.update_current_clock("Send to server", 0)
        
        msg_dict = {'type': 'quit',
                    'timestamp': self.timestamp.get_dict()}
        
        self.get_response_from_server(msg_dict, client_socket)
        logger.info("Bye..have a good one!")


if __name__ == '__main__':
    client_id = sys.argv[1]
    with open(os.path.join(pathlib.Path(__file__).parent.resolve(),'config.json'), 'r') as config_file:
        config_info = json.load(config_file)
        clients_info = config_info["clients"]
        if client_id not in clients_info:
            logger.error("Invalid client id. Please check...")
        else:
            logger.info("Initiating client..")
            Client(client_id, clients_info)