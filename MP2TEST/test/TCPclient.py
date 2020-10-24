#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 19 16:30:17 2020

@author: maye
"""

import socket
import sys

HOST, PORT = 'fa20-cs425-g29-01.cs.illinois.edu a.txt', 9999
data = " ".join("This is me!")

test = "CONFIRM fa20-cs425-g29-01.cs.illinois.edu a.txt"
strip = test.split(' ')
print(strip[0])
print(strip[1])
print(strip[2])

# Create a socket (SOCK_STREAM means a TCP socket)
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    # Connect to server and send data
    sock.connect((HOST, PORT))
    sock.sendall(bytes(data + "\n", "utf-8"))

    # Receive data from the server and shut down
    received = str(sock.recv(1024), "utf-8")

print("Sent:     {}".format(data))
print("Received: {}".format(received))