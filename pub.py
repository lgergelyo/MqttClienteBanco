# import context  # Ensures paho is in PYTHONPATH
import paho.mqtt.publish as publish

publish.single("paho/test","boo", hostname="localhost")