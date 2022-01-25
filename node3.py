import json
import pickle
import socket
import threading
import time
import numpy

STATES = ["LEADER", "CANDIDATE", "FOLLOWER"]
SERVER_ADDRESS = [("127.0.0.1", 9031), ("127.0.0.1", 9032), ("127.0.0.1", 9033)]
ClientListener = ("127.0.0.1", 9998)
current_state = STATES[1]
dict = {}
log_file = []
# order is : s1 - s2 - (connection_num1, ClientAddress_num1) - (connection_num2, ClientAddress_num2)
my_connections = []

def leader_func(server_socket):
    print("start leader function...\n")
    connection_num1 = my_connections[2][0]
    ClientAddress_num1 = my_connections[2][1]
    connection_num2 = my_connections[3][0]
    ClientAddress_num2 = my_connections[3][1]

    leader_message = ("This is a message from LEADER with port:" + str(SERVER_ADDRESS[2][1])).encode('utf-8')
    connection_num1.send(leader_message)
    connection_num2.send(leader_message)

    th1 = threading.Thread(target=leader_receive_follower1_message, args=[connection_num1,connection_num2])
    th1.start()

    th2 = threading.Thread(target=leader_receive_follower2_message, args=[connection_num1,connection_num2])
    th2.start()

    while True:
        print("\nListening...\nWaiting for a connection ...\n")

        client_connection, client_address = server_socket.accept()

        print("connection received from: ", client_address)

        th3 = threading.Thread(target=leader_receive_client_message, args=[client_connection, client_address, connection_num1, connection_num2])
        th3.start()


def follower_func(s1, s2, server_socket):
    print("start follower function...\n")
    s1.settimeout(10)
    s2.settimeout(10)

    try:
        leader_message1 = s1.recv(1024).decode("ascii")
        print(leader_message1)
        leader_port = int(leader_message1.split(":")[1])

    except:
        leader_message2 = s2.recv(1024)
        print(leader_message2.decode("ascii"))
        leader_port = int(leader_message2.decode("ascii").split(":")[1])

    s1.settimeout(None)
    s2.settimeout(None)

    if leader_port == SERVER_ADDRESS[0][1]:
        s = s1
    else:
        s = s2

    th1 = threading.Thread(target=follower_receive_leader_message, args=[s])
    th1.start()

    while True:
        print("\nListening...\nWaiting for a connection ...\n")

        client_connection, client_address = server_socket.accept()

        print("connection received from: ", client_address)

        th2 = threading.Thread(target=follower_receive_client_message, args=[client_connection, client_address, s])
        th2.start()

def follower_receive_leader_message(s):
    while 1:
        message = s.recv(1024)
        print("*** consensus : log received ***")
        message = message.decode('utf-8')
        log_file = eval(message)
        print(log_file)

def follower_receive_client_message(client_connection, client_address, s):
    print("*** client message ***")
    message = client_connection.recv(1024)
    print("message received from client...\n")
    # message = pickle.loads(message)
    s.sendall(message)
    print("client message sent to leader\n")

def leader_receive_follower1_message(connection_num1,connection_num2):
    while 1:

        message = connection_num1.recv(1024)
        message = pickle.loads(message)
        print(type(message))
        print(message)
        print()
        log_file.append(message)

        connect_to_response = socket.socket()
        connect_to_response.connect(ClientListener)
        connect_to_response.send(b"Your data was received by FOLLOWER server and also saved successfully!")
        if len(log_file) % 3 == 0:
            data = str(log_file)
            data = data.encode()
            connection_num1.send(data)
            connection_num2.send(data)
        connect_to_response.close()

def leader_receive_follower2_message(connection_num1,connection_num2):
    while 1:

        message = connection_num2.recv(1024)
        message = pickle.loads(message)
        print(type(message))
        print(message)
        print()
        log_file.append(message)

        connect_to_response = socket.socket()
        connect_to_response.connect(ClientListener)
        connect_to_response.send(b"Your data was received by FOLLOWER server and also saved successfully!")
        if len(log_file) % 3 == 0:
            data = str(log_file)
            data = data.encode()
            connection_num1.send(data)
            connection_num2.send(data)
        connect_to_response.close()

def leader_receive_client_message(client_connection, client_address, connection_num1, connection_num2):

    message = client_connection.recv(1024)
    message = pickle.loads(message)
    print(type(message))
    print(message)
    print()
    log_file.append(message)
    connect_to_response = socket.socket()
    connect_to_response.connect(ClientListener)
    connect_to_response.send(b"Your data was received by LEADER server and also saved successfully!")

    if len(log_file) % 3 == 0:
        data = str(log_file)
        data = data.encode()
        connection_num1.send(data)
        connection_num2.send(data)
        print("log is sent to followers\n")
    connect_to_response.close()

def ReceiveMessage(connection, client_address):
    global current_state
    message = float(connection.recv(1024).decode('ascii'))
    print(client_address[1])
    dict[client_address[1]] = message
    print(dict)

    while True:
        if len(dict) == 3:
            break

    leader = max(dict, key=dict.get)
    print("\nleader: ", leader)
    if leader == SERVER_ADDRESS[2][1]:
        current_state = STATES[0]
    else:
        current_state = STATES[2]
    print("\ncurrent satate: ", current_state)

def main():
    server_socket = socket.socket()
    server_socket.bind(SERVER_ADDRESS[2])
    server_socket.listen(10)
    random_number = numpy.random.random()
    dict[SERVER_ADDRESS[2][1]] = random_number

    s1 = socket.socket()
    s1.bind(("127.0.0.1", 5015))
    s1.connect(SERVER_ADDRESS[0])
    my_connections.append(s1)
    s1.send(str(random_number).encode('utf-8'))

    s2 = socket.socket()
    s2.bind(("127.0.0.1", 5016))
    s2.connect(SERVER_ADDRESS[1])
    my_connections.append(s2)
    s2.send(str(random_number).encode('utf-8'))

    counter = 0
    while True:
        print("\nListening...\nWaiting for connection ...\n")
        connection, client_address = server_socket.accept()
        my_connections.append((connection, client_address))
        print("connection received from: ", client_address)

        th = threading.Thread(target=ReceiveMessage, args=[connection, client_address])
        th.start()
        counter += 1
        if counter == 2:
            time.sleep(10)
            break

    if current_state == STATES[0]:
        leader_func(server_socket)

    elif current_state == STATES[2]:
        follower_func(s1, s2, server_socket)

print()
if __name__ == "__main__":
    main()

