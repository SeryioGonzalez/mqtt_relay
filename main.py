import json
import logging
import os
import paho.mqtt.client as paho
import signal
import sys
import threading
from azure.iot.device import IoTHubModuleClient

log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(level=log_level)

default_iot_edge_output_name = 'output1'
default_mqtt_topic = "/{}/{}".format( os.environ.get("IOTEDGE_DEVICEID"), os.environ.get("IOTEDGE_MODULEID"))
default_mqtt_port = 1883

iot_edge_output_name = os.environ.get("OUTPUT_NAME", default_iot_edge_output_name)

DESTINATION_MQTT_TOPIC       = os.environ.get("DESTINATION_MQTT_TOPIC", default_mqtt_topic)
DESTINATION_MQTT_BROKER_FQDN = os.environ.get("DESTINATION_MQTT_BROKER_FQDN")
DESTINATION_MQTT_BROKER_PORT = os.environ.get("DESTINATION_MQTT_BROKER_PORT", default_mqtt_port)

MQTT_CLIENT_NAME = "{}@{}".format( os.environ.get("IOTEDGE_MODULEID"), os.environ.get("IOTEDGE_DEVICEID"))

def process_input_message(input_message):
    logging.info("INPUT - Preprocessing incoming message")
    logging.debug("INPUT - Input message is {}".format(input_message))

    #ANY INPUT MESSAGE PROCESSING SHOULD HAPPEN HERE
    output_data = input_message
    logging.debug("INPUT - Preprocessed message: {}".format(output_data))
    logging.info("INPUT - Processed incoming message")

    return output_data

def init_mqtt_client():
    logging.info("INIT - Connecting MQTT CLIENT {} to MQTT BROKER {} ON PORT {}".format(MQTT_CLIENT_NAME, DESTINATION_MQTT_BROKER_FQDN, DESTINATION_MQTT_BROKER_PORT))
    mqtt_client = paho.Client(MQTT_CLIENT_NAME)   
    mqtt_client.on_publish = publish_message_to_mqtt_broker
    mqtt_client.connect(DESTINATION_MQTT_BROKER_FQDN, DESTINATION_MQTT_BROKER_PORT)  

    return mqtt_client

def publish_message_to_mqtt_broker(mqtt_client, userdata, result):
    logging.info("MQTT - Data published to broker")
    logging.debug("MQTT - Data published to broker was {}".format(userdata))
    logging.debug("MQTT - Published result was {}".format(result))

def init_iot_edge_module():
    iot_edge_client = IoTHubModuleClient.create_from_edge_environment()
    mqtt_client = init_mqtt_client()

    def receive_message_handler(message):
        
        logging.info("INPUT - Message received on input {}".format(message.input_name))
        output_message = process_input_message(message.data)

        #SENDING IT TO EDGEHUB
        logging.info("OUTPUT - Forwarding message to output {}".format(iot_edge_output_name))
        iot_edge_client.send_message_to_output(str(output_message), iot_edge_output_name)

        #SENDING IT TO MQTT BROKER
        logging.info("MQTT - Publishing message to topic {}".format(DESTINATION_MQTT_TOPIC))
        mqtt_client.publish(DESTINATION_MQTT_TOPIC, str(output_message) )
        logging.info("MQTT - Published message")

    try:
        logging.info("INIT - Registering incoming message handler" )
        iot_edge_client.on_message_received = receive_message_handler
    except:
        # Cleanup
        logging.info("ERROR - Registering message handler" )
        iot_edge_client.shutdown()

    return iot_edge_client

if __name__ == '__main__':
    logging.info("INIT - IoT Edge Data Parser" )

    # Event indicating sample stop
    logging.info("INIT - Creating Stop event" )
    stop_event = threading.Event()

    # Define a signal handler that will indicate Edge termination of the Module
    def module_termination_handler(signal, frame):
        logging.info("STOP - IoTHubClient sample stopped")
        stop_event.set()

    # Attach the signal handler
    logging.info("INIT - Registering Stop Event" )
    signal.signal(signal.SIGTERM, module_termination_handler)

    # Create the client
    logging.info("INIT - Creating IoT Edge Module Client" )
    client = init_iot_edge_module()

    try:
        # This will be triggered by Edge termination signal
        stop_event.wait()
    except Exception as e:
        logging.info("ERROR - Unexpected error %s " % e)
        raise
    finally:
        # Graceful exit
        logging.info("STOP - Shutting down client")
        client.shutdown()
