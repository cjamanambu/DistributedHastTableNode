#!/usr/bin/python

import random
import socket
import sys
import json
import datetime
import time
import select

def initialized_node():
    new_node = Node()
    new_node.SOCK = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        new_node.SOCK.bind(new_node.ADDR)
    except Exception as e:
        print("Socket Bind Error:", e)
        sys.exit(1)

    log_message = "New Node Created!"
    log_message += "\nID: " + str(new_node.ID)
    log_message += "\nBound to: "
    log_message += "\n[Hostname: " + new_node.ADDR[0]
    log_message += ", Port: " + str(new_node.ADDR[1]) + "]"
    new_node.log('Activity', log_message)

    return new_node

def run(new_node):
    timer = 0
    while True:
        socket_fd = new_node.SOCK.fileno()
        input_list = [socket_fd, sys.stdin]
        read_fd, write_fd, error_fd = select.select(input_list, [], [], 0.0)

        if new_node.IN_RING is False:
            new_node.log('Activity', 'Joining the DHT Ring...')
            new_node.join_ring(new_node.BOOTSTRAP_ADDR)

        for input_point in read_fd:
            if input_point == sys.stdin:
                query = sys.stdin.readline()
                log_message = 'Received Input from stdin'
                if query == '\n':
                    log_message += '\nEnter key was pressed\nShutdown sequence initiated'
                    new_node.log('Activity', log_message)
                    new_node.shutdown()
                else:
                    log_message += '\nInput received: ' + str(query)
                    try:
                        query = int(query)
                    except ValueError:
                        log_message += "\nA non integer value was received"
                        new_node.INVALID_QUERY = True
                    else:
                        if query < new_node.ID or query == new_node.ID:
                            log_message += "\nThe integer value was less than or equal to my ID"
                            new_node.INVALID_QUERY = True

                    if new_node.INVALID_QUERY is True:
                        log_message += '\nHold on generating valid query...'
                        query = new_node.generate_valid_query()
                        log_message += '\nValid query generated: '+ str(query)
                        new_node.log('Activity', log_message)
                        new_node.generate_find_protocol(query, None, True)
                        new_node.INVALID_QUERY = False
                    else:
                        log_message += '\nValid query was received'
                        new_node.log('Activity', log_message)
                        new_node.generate_find_protocol(query, None, True)

            elif input_point == socket_fd:
                try:
                    new_node.SOCK.settimeout(10)
                    server_request, address = new_node.SOCK.recvfrom(4096)
                    new_node.handle_responses(server_request, address)
                except socket.timeout:
                    print

        time.sleep(0.5)
        timer += 0.5
        if timer == 30:
            new_node.stabilize_ring()
            timer = 0

def generate_timestamp():
    return datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')


def build_msg(cmd, port, node_id, host, query, hops):
    msg = {}

    msg['cmd'] = cmd
    msg['port'] = port
    msg['ID'] = node_id
    msg['hostname'] = host

    if cmd == 'find' or cmd == 'owner':
        msg['query'] = query
        msg['hops'] = hops

    return json.dumps(msg)


class Node:

    def __init__(self):
        self.PORT = 15047
        self.HOST = socket.gethostname()
        self.ID = random.randint(1, (2 **16 - 2))
        self.SUCC = {}
        self.PRED = {"hostname": 'silicon.cs.umanitoba.ca', "port": 15000, "ID": 0}
        self.ADDR = (self.HOST, self.PORT)
        self.BOOTSTRAP_ADDR = ('silicon.cs.umanitoba.ca', 15000)
        self.SOCK = None
        self.LAST_ACTIVE_RESPONSE = {}
        self.LOG_COUNT = 0
        self.IN_RING = False
        self.EXIST_HANDLER = False
        self.STABILIZING = False
        self.STAB_TIMEOUT = False
        self.INVALID_QUERY = False
        self.REJOIN = False

    def set_succ(self, new_succ):
        self.SUCC = new_succ

    def get_succ(self):
        return self.SUCC

    def get_pred(self):
        return self.PRED

    def get_logno(self):
        self.LOG_COUNT += 1
        return self.LOG_COUNT

    def generate_valid_query(self):
        valid_query = -1
        while valid_query <= self.ID:
            valid_query = random.randint(self.ID, (2 **16 - 1))
        return valid_query

    def obj_transform(self):
        obj = {}
        obj['hostname'] = self.HOST
        obj['port'] = self.PORT
        obj['ID'] = self.ID
        return obj

    def generate_find_protocol(self, query, find_message, mine):
        try:
            cmd_type = 'find'
            address = (self.get_succ()['hostname'], self.get_succ()['port'])
            if mine is True:
                self.send_msg(cmd_type, build_msg(cmd_type, self.PORT, self.ID, self.HOST, query, 0), address)
            elif mine is False:
                self.send_msg(cmd_type, json.dumps(find_message), address)
        except Exception as e:
            print "Error while generating find protocol:", e

    def generate_owner_protocol(self, find_message):
        try:
            find_message['hops'] += 1
            if type(find_message['query']) is int:
                if find_message['query'] != self.ID:
                    self.generate_find_protocol(find_message['query'], find_message, False)
                elif find_message['query'] == self.ID:
                    cmd_type = 'owner'
                    query = find_message['query']
                    hops = find_message['hops']
                    address = (find_message['hostname'], find_message['port'])
                    self.send_msg(cmd_type, build_msg(cmd_type, self.PORT, self.ID, self.HOST, query, hops), address)
        except Exception as e:
            log_message = 'Error while generating owner protocol:'
            log_message += '\n'+str(e)
            self.log('Error', log_message)

    def handle_responses(self, server_request, address):
        try:
            if server_request:
                server_request = json.loads(server_request)
                if 'cmd' in server_request:
                    cmd = server_request['cmd']
                    if cmd == "pred?":
                        log_message = "RECEIVED pred? Protocol from:"
                        log_message += "\n[Hostname: " + server_request['hostname']
                        log_message += ", Port: " + str(server_request['port'])
                        log_message += ', ID: ' + str(server_request['ID']) + ']'
                        log_message += "\nMy predecessor is this: "
                        log_message += "\n[Hostname: " + self.get_pred()['hostname']
                        log_message += ", Port: " + str(self.get_pred()['port'])
                        log_message += ', ID: ' + str(self.get_pred()['ID']) + ']'

                        address = (server_request['hostname'], server_request['port'])
                        cmd_type = "myPred"
                        response_message = {}
                        response_message['me'] = self.obj_transform()
                        response_message['cmd'] = 'myPred'
                        response_message['thePred'] = self.get_pred()
                        self.log('Activity', log_message)
                        self.send_msg(cmd_type, json.dumps(response_message), address)

                    elif cmd == 'setPred':
                        log_message = "RECEIVED setPred Protocol from:"
                        log_message += "\n[Hostname: " + server_request['hostname']
                        log_message += ", Port: " + str(server_request['port'])
                        log_message += ', ID: ' + str(server_request['ID']) + ']'
                        self.PRED['hostname'] = server_request['hostname']
                        self.PRED['port'] = server_request['port']
                        self.PRED['ID'] = server_request['ID']
                        log_message += '\nSetting my predecessor to: '
                        log_message += "\n[Hostname: " + self.PRED['hostname']
                        log_message += ", Port: " + str(self.PRED['port'])
                        log_message += ', ID: ' + str(self.PRED['ID']) + ']'
                        self.log('Activity', log_message)

                    elif cmd == 'myPred':
                        log_message = "RECEIVED myPred Protocol from:"
                        log_message += "\n[Hostname: " + server_request['me']['hostname']
                        log_message += ", Port: " + str(server_request['me']['port'])
                        log_message += ', ID: ' + str(server_request['me']['ID']) + ']'
                        log_message += '\nNote: Received message at the wrong point.\nResending pred? message'
                        self.log('Activity', log_message)
                        address = (server_request['me']['hostname'], server_request['me']['port'])
                        self.join_ring(address)

                    elif cmd == 'find':
                        log_message = "RECEIVED find Protocol from:"
                        log_message += "\n[Hostname: " + server_request['hostname']
                        log_message += ", Port: " + str(server_request['port'])
                        log_message += ', ID: ' + str(server_request['ID']) + ']'
                        log_message += "\nProcessing..."
                        self.log('Activity', log_message)
                        self.generate_owner_protocol(server_request)

                    elif cmd == 'owner':
                        log_message = "RECEIVED owner Protocol from:"
                        log_message += "\n[Hostname: " + server_request['hostname']
                        log_message += ", Port: " + str(server_request['port'])
                        log_message += ', ID: ' + str(server_request['ID']) + ']'
                        log_message += "\nNumber of hops: "+ str(server_request['hops'])
                        log_message += "\nQuery: "+ str(server_request['query'])
                        self.log('Activity', log_message)
        except Exception as e:
            log_message = 'Error while handling responses:'
            log_message += '\n'+str(e)
            self.log('Error', log_message)

    def send_msg(self, cmd_type, message, address):
        server_response = ""
        socket_timeout = False
        log_message = 'Sending a Message to: '
        log_message += '\n[Hostname: ' + str(address[0])
        log_message += ', Port: ' + str(address[1]) + ']'

        self.SOCK.sendto(message, address)

        if cmd_type == "pred?":
            log_message += '\nMessage cmd: pred? Awaiting response\nTimeout: 2secs\n...'
            self.SOCK.settimeout(2)
            try:
                server_response, server_address = self.SOCK.recvfrom(4096)
                server_response = json.loads(server_response)
                if 'me' in server_response and 'thePred' in server_response:
                    log_message += '\nResponse received, Predecessor: '
                    log_message += '\n[Hostname: ' + str(server_response['thePred']['hostname'])
                    log_message += ', Port: ' + str(server_response['thePred']['port']) + ']'

                    if int(server_response['thePred']['port']) != self.PORT:
                        self.LAST_ACTIVE_RESPONSE['me'] = server_response['me']
                        log_message += '\nSaving last known node as: '
                        log_message += '\n[Hostname: ' + str(server_response['me']['hostname'])
                        log_message += ', Port: ' + str(server_response['me']['port'])
                        log_message += ', ID: ' + str(server_response['me']['ID']) + ']'
                    else:
                        log_message += '\nNOTE: Predecessor is on the same port! Could it be me??'
                        self.EXIST_HANDLER = True
                else:
                    log_message += '\nNote: Unexpected message received while waiting, Handling it...'
                    log_message += '\nMessage: ' + str(server_response)
                    self.handle_responses(json.dumps(server_response), address)
            except socket.timeout:
                socket_timeout = True
                log_message = "\nThe socket timed out while waiting for pred? response"
                if self.STABILIZING is False:
                    self.LAST_ACTIVE_RESPONSE['thePred'] = "Unresponsive"
                    server_response = self.LAST_ACTIVE_RESPONSE
                    log_message += "\nSending setPred to last known node: \n"
                    log_message += '\n[Hostname: ' + str(server_response['me']['hostname'])
                    log_message += ', Port: ' + str(server_response['me']['port'])
                    log_message += ', ID: ' + str(server_response['me']['ID']) + ']'

                elif self.STABILIZING is True:
                    self.STAB_TIMEOUT = True
                    log_message += "\nNode was stabilizing\nInitiating join at bootstrap.."

        elif cmd_type == "setPred":
            log_message += '\nMessage cmd: setPred'
            if self.REJOIN is True:
                log_message += '\nNote: This is a REJOIN'
                log_message += '\nMy Current Successor:'
                log_message += '\n[Hostname: ' + str(self.get_succ()['hostname'])
                log_message += ', Port: ' + str(self.get_succ()['port'])
                log_message += ', ID: ' + str(self.get_succ()['ID']) + ']'
                log_message += '\nMy Current Predecessor:'
                log_message += '\n[Hostname: ' + str(self.PRED['hostname'])
                log_message += ', Port: ' + str(self.PRED['port'])
                log_message += ', ID: ' + str(self.PRED['ID']) + ']'

        elif cmd_type == "myPred":
            log_message += '\nMessage cmd: myPred'

        elif cmd_type == "find":
            log_message += '\nMessage cmd: find'

        elif cmd_type == "owner":
            log_message += '\nMessage cmd: owner'

        if socket_timeout is True:
            self.log('Error', log_message)
        else:
            self.log('Activity', log_message)

        return server_response

    def join_ring(self, address):

        cmd_type = "pred?"
        server_response = self.send_msg(cmd_type, build_msg(cmd_type, self.PORT, self.ID, self.HOST, None, None), address)

        if server_response:
            try:
                server_response_predecessor = server_response['thePred']
                server_response = server_response['me']
                if self.EXIST_HANDLER is False:

                    if server_response_predecessor != "Unresponsive":
                        server_response_predecessor_id = int(server_response_predecessor['ID'])
                        server_response_predecessor_hostname = str(server_response_predecessor['hostname'])
                        server_response_predecessor_port = int(server_response_predecessor['port'])
                        if server_response_predecessor_id < self.ID:
                            cmd_type = "setPred"
                            self.send_msg(cmd_type, build_msg(cmd_type, self.PORT, self.ID, self.HOST, None, None), address)
                            self.set_succ(server_response)
                            self.IN_RING = True
                        else:
                            self.join_ring((server_response_predecessor_hostname, server_response_predecessor_port))
                    else:
                        cmd_type = "setPred"
                        address = (server_response['hostname'], server_response['port'])
                        self.send_msg(cmd_type, build_msg(cmd_type, self.PORT, self.ID, self.HOST, None, None), address)
                        self.set_succ(server_response)
                        self.IN_RING = True
                else:
                    if server_response_predecessor['ID'] == self.ID:
                        print 'Its a current version of me\nDo nothing for now'
                    else:
                        print 'Its a previous version of me\nSending setPred to my Successor\n'
                        self.EXIST_HANDLER = False
                        cmd_type = "setPred"
                        self.send_msg(cmd_type, build_msg(cmd_type, self.PORT, self.ID, self.HOST, None, None), address)
                        self.set_succ(server_response)
                    self.IN_RING = True
                    self.REJOIN = True
            except Exception as e:
                log_message = 'Error while joining the ring:'
                log_message += '\n'+str(e)
                self.log('Error', log_message)


    def stabilize_ring(self):
        try:
            self.STABILIZING = True
            self.log('Activity', '30 secs Elapsed, Began Stabilizing')
            cmd_type = "pred?"
            address = (self.get_succ()['hostname'], self.get_succ()['port'])
            server_response = self.send_msg(cmd_type, build_msg(cmd_type, self.PORT, self.ID, self.HOST, None, None), address)
            if self.STAB_TIMEOUT is False:
                server_response_predecessor = server_response['thePred']
                server_response_predecessor_id = int(server_response_predecessor['ID'])
                server_response_predecessor_hostname = str(server_response_predecessor['hostname'])
                server_response_predecessor_port = int(server_response_predecessor['port'])
                if server_response_predecessor_id > self.ID:
                    cmd_type = "setPred"
                    address = (server_response_predecessor_hostname, server_response_predecessor_port)
                    self.send_msg(cmd_type, build_msg(cmd_type, self.PORT, self.ID, self.HOST, None, None), address)
                    self.set_succ(server_response_predecessor)
                elif server_response_predecessor_id < self.ID:
                    cmd_type = "setPred"
                    self.send_msg(cmd_type, build_msg(cmd_type, self.PORT, self.ID, self.HOST, None, None), address)
            else:
                self.IN_RING = False
                self.STAB_TIMEOUT = False
            self.STABILIZING = False
            self.log('Activity', 'Finished Stabilizing')
        except Exception as e:
            log_message = 'Error while stabilizing the ring:'
            log_message += '\n'+str(e)
            self.log('Error', log_message)

    def shutdown(self):
        self.SOCK.close()
        sys.exit(1)

    def log(self, event_type, log_message):
        print
        print "********************************************************************************"
        if event_type == 'Activity':
            print "ACTIVITY: log %d [timestamp: %s]" % (self.get_logno(), generate_timestamp())
        elif event_type == 'Error':
            print "ERROR: log %d [timestamp: %s]" % (self.get_logno(), generate_timestamp())
        print "-------------------------------------------------------"
        print "LOG MESSAGE:"
        print log_message
        print "********************************************************************************"
        print


def main():
    run(initialized_node())


if __name__ == '__main__':
    main()
