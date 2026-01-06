#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" Example code for sending and receiving MQTT messages to/from AWS IoT Core using Paho MQTT library.
The command line arguments are similar to that used in the AWS IOT Thing connect_device_package script, downloaded when creating a new IoT Thing.
This script connects to the specified AWS endpoint using the provided CA certificate, client certificate, and private key.
It subscribes to the topic "sdk/test/python", publishes a message to the same topic, and waits to receive the message back.
"""

import paho.mqtt.client as mqtt
import threading
import argparse
import logging

rdSem = threading.Semaphore(0)
wrSem = threading.Semaphore(0)
connectSem = threading.Semaphore(0)

def on_connect(self, userdata, flags, rc, properties=None):
    connectSem.release()

def on_publish(self, userdata, msg):
    wrSem.release()

def on_message(self, userdata, msg):
    print(msg.topic, str(msg.payload))
    rdSem.release()

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Sends a message as basicPubSub under topic sdk/test/python to the specified AWS endpoint")
    parser.add_argument('--endpoint', required=True, help='The endpoint url to which to connect')
    parser.add_argument('--ca_cert', required=True, help='The root CA certificate file path')
    parser.add_argument('--cert', required=True, help='The client certificate file path')
    parser.add_argument('--key', required=True, help='The client private key file path')
    opt = parser.parse_args()
    
    client = mqtt.Client("basicPubSub", protocol=mqtt.MQTTv5)
    logging.basicConfig(level=logging.DEBUG)
    client.enable_logger()
    client.on_connect = on_connect
    client.on_connect_fail = on_connect
    client.on_message = on_message
    client.on_publish = on_publish


    client.tls_set(ca_certs= opt.ca_cert, certfile= opt.cert, keyfile= opt.key)  
    res = client.connect(opt.endpoint, 8883)
    if res != mqtt.MQTT_ERR_SUCCESS:
        print("Connection failed with error code ", res)
        exit(-1)
    client.loop_start()

    connectSem.acquire()
    result, mID = client.subscribe("sdk/test/python")
    if result != mqtt.MQTT_ERR_SUCCESS:
        print("Subscription failed with error code ", result)
        exit(-1)
    result, mID = client.publish("sdk/test/python", "Hello from sendMQTT_to_AWS.py", qos=1)
    if result != mqtt.MQTT_ERR_SUCCESS:
        print("Publish failed with error code ", result)
        exit(-1)
    wrSem.acquire()
    rdSem.acquire()
    client.loop_stop()
    client.disconnect()
