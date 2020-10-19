#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 19 20:43:47 2020

@author: maye
"""

import socket
import threading
import logging

if __name__ == '__main__':
    logger = logging.getLogger("client")
    logger.debug('creating socket')
    #s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    logger.debug('connecting to server')
    ip = "127.0.0.1"
    port = 62536
    
    """
    本脚本用于用户模拟client端发送的各种请求。样例：
    ASSIGN a.txt
    CONFIRM fa20-cs425-g29-10.cs.illinois.edu a.txt
    FIND a.txt
    PURGE a.txt
    """
    while True:
        instr = input("Please input command:")
        if instr == "stop":
            break
        print(instr)
        message = instr.encode()
        logger.debug('sending data: %r', message)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((ip, port))
        len_sent = s.send(message)

        # 接收响应
        logger.debug('waiting for response')
        response = s.recv(1024)
        print("response is :{}".format(response.decode()))

    # 执行清理
    logger.debug('closing socket')
    s.close()
    logger.debug('done')