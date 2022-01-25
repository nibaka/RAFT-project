import json
import pickle
import random
import socket

SERVER_ADDRESS = [("127.0.0.1", 9031), ("127.0.0.1", 9032), ("127.0.0.1", 9033)]
ClientListener = ("127.0.0.1", 9998)
BUFFER_SIZE = 4096 #4KB

def RandomServer():
    index = random.randint(0, 2)
    return SERVER_ADDRESS[index]

def main():
    # s : A socket for accept connection from responsive server
    s = socket.socket()
    s.bind(ClientListener)
    s.listen()

    dynamic_ip = 7001
    for i in range(3):
        clientSocket = socket.socket()
        clientSocket.bind(("127.0.0.1", dynamic_ip))
        server_address = RandomServer()
        clientSocket.connect(server_address)
        print("connect to server: ", server_address)
        # request
        message_key = input("Enter your key:")
        print()
        message_value = input("Enter your value:")
        my_request = {message_key: message_value}
        my_request = pickle.dumps(my_request)
        # send the request
        clientSocket.sendall(my_request)
        print("\nrequest is sent!")
        # receive server response
        conn, addr = s.accept()

        server_response = conn.recv(1024).decode('ascii')
        # server_response = clientSocket.recv(1024).decode('ascii')
        print("\nresponse:")
        print(server_response)
        conn.close()
        dynamic_ip += 1
        clientSocket.close()
        conn.close()

    s.close()

if __name__ == "__main__":
    main()

