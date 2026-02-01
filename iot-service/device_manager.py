import paho.mqtt.client as mqtt
import json
import logging
from datetime import datetime
from typing import Dict, Any, Callable
import asyncio

logger = logging.getLogger(__name__)

class IoTDeviceManager:
    """
    IoT Device Manager for smart home automation.
    Handles device discovery, control, and state management via MQTT.
    """
    
    def __init__(self, broker_host='localhost', broker_port=1883):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client = mqtt.Client()
        self.devices = {}
        self.callbacks = {}
        
        # MQTT callbacks
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        
    def connect(self):
        """Connect to MQTT broker"""
        try:
            self.client.connect(self.broker_host, self.broker_port, 60)
            self.client.loop_start()
            logger.info(f"Connected to MQTT broker at {self.broker_host}:{self.broker_port}")
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            raise
    
    def disconnect(self):
        """Disconnect from MQTT broker"""
        self.client.loop_stop()
        self.client.disconnect()
        logger.info("Disconnected from MQTT broker")
    
    def _on_connect(self, client, userdata, flags, rc):
        """Callback when connected to MQTT broker"""
        if rc == 0:
            logger.info("Successfully connected to MQTT broker")
            # Subscribe to device discovery topic
            client.subscribe("smarthome/devices/+/status")
            client.subscribe("smarthome/devices/+/state")
            client.subscribe("smarthome/discovery/#")
        else:
            logger.error(f"Failed to connect to MQTT broker with code: {rc}")
    
    def _on_disconnect(self, client, userdata, rc):
        """Callback when disconnected from MQTT broker"""
        if rc != 0:
            logger.warning(f"Unexpected disconnection from MQTT broker: {rc}")
    
    def _on_message(self, client, userdata, msg):
        """Callback when message received from MQTT broker"""
        try:
            topic = msg.topic
            payload = json.loads(msg.payload.decode())
            
            logger.debug(f"Received message on topic {topic}: {payload}")
            
            # Handle device discovery
            if topic.startswith("smarthome/discovery/"):
                self._handle_device_discovery(topic, payload)
            
            # Handle device state updates
            elif topic.endswith("/state"):
                device_id = topic.split('/')[2]
                self._handle_device_state_update(device_id, payload)
            
            # Handle device status
            elif topic.endswith("/status"):
                device_id = topic.split('/')[2]
                self._handle_device_status(device_id, payload)
            
            # Trigger callbacks
            if topic in self.callbacks:
                for callback in self.callbacks[topic]:
                    callback(topic, payload)
                    
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def _handle_device_discovery(self, topic, payload):
        """Handle device discovery messages"""
        device_id = payload.get('device_id')
        device_type = payload.get('type')
        device_name = payload.get('name')
        
        if device_id and device_id not in self.devices:
            self.devices[device_id] = {
                'id': device_id,
                'type': device_type,
                'name': device_name,
                'capabilities': payload.get('capabilities', []),
                'manufacturer': payload.get('manufacturer'),
                'model': payload.get('model'),
                'firmware_version': payload.get('firmware_version'),
                'discovered_at': datetime.now().isoformat(),
                'online': True,
                'state': {}
            }
            
            logger.info(f"Discovered new device: {device_name} ({device_id})")
            
            # Subscribe to device-specific topics
            self.client.subscribe(f"smarthome/devices/{device_id}/#")
    
    def _handle_device_state_update(self, device_id, payload):
        """Handle device state update messages"""
        if device_id in self.devices:
            self.devices[device_id]['state'].update(payload)
            self.devices[device_id]['last_update'] = datetime.now().isoformat()
            logger.debug(f"Updated state for device {device_id}: {payload}")
    
    def _handle_device_status(self, device_id, payload):
        """Handle device status messages"""
        if device_id in self.devices:
            self.devices[device_id]['online'] = payload.get('online', False)
            self.devices[device_id]['battery'] = payload.get('battery')
            self.devices[device_id]['signal_strength'] = payload.get('signal_strength')
            logger.debug(f"Updated status for device {device_id}: {payload}")
    
    def get_devices(self, device_type=None):
        """Get all devices or filter by type"""
        if device_type:
            return {k: v for k, v in self.devices.items() if v['type'] == device_type}
        return self.devices
    
    def get_device(self, device_id):
        """Get specific device by ID"""
        return self.devices.get(device_id)
    
    def control_device(self, device_id, command, params=None):
        """Send control command to device"""
        if device_id not in self.devices:
            raise ValueError(f"Device {device_id} not found")
        
        topic = f"smarthome/devices/{device_id}/command"
        payload = {
            'command': command,
            'params': params or {},
            'timestamp': datetime.now().isoformat()
        }
        
        self.client.publish(topic, json.dumps(payload))
        logger.info(f"Sent command '{command}' to device {device_id}")
        
        return True
    
    def turn_on(self, device_id):
        """Turn on a device"""
        return self.control_device(device_id, 'turn_on')
    
    def turn_off(self, device_id):
        """Turn off a device"""
        return self.control_device(device_id, 'turn_off')
    
    def set_brightness(self, device_id, brightness):
        """Set brightness for a light (0-100)"""
        return self.control_device(device_id, 'set_brightness', {'brightness': brightness})
    
    def set_color(self, device_id, color):
        """Set color for a light (RGB or hex)"""
        return self.control_device(device_id, 'set_color', {'color': color})
    
    def set_temperature(self, device_id, temperature):
        """Set temperature for a thermostat"""
        return self.control_device(device_id, 'set_temperature', {'temperature': temperature})
    
    def lock(self, device_id):
        """Lock a smart lock"""
        return self.control_device(device_id, 'lock')
    
    def unlock(self, device_id):
        """Unlock a smart lock"""
        return self.control_device(device_id, 'unlock')
    
    def create_scene(self, scene_name, device_states):
        """
        Create a scene with multiple device states
        
        Args:
            scene_name: Name of the scene
            device_states: Dict of device_id -> state mappings
        """
        topic = "smarthome/scenes/create"
        payload = {
            'name': scene_name,
            'devices': device_states,
            'created_at': datetime.now().isoformat()
        }
        
        self.client.publish(topic, json.dumps(payload))
        logger.info(f"Created scene: {scene_name}")
        
        return True
    
    def activate_scene(self, scene_name):
        """Activate a scene"""
        topic = "smarthome/scenes/activate"
        payload = {
            'name': scene_name,
            'timestamp': datetime.now().isoformat()
        }
        
        self.client.publish(topic, json.dumps(payload))
        logger.info(f"Activated scene: {scene_name}")
        
        return True
    
    def create_automation(self, automation_name, trigger, conditions, actions):
        """
        Create an automation rule
        
        Args:
            automation_name: Name of the automation
            trigger: Trigger condition (time, event, state change)
            conditions: List of conditions to check
            actions: List of actions to execute
        """
        topic = "smarthome/automations/create"
        payload = {
            'name': automation_name,
            'trigger': trigger,
            'conditions': conditions,
            'actions': actions,
            'created_at': datetime.now().isoformat()
        }
        
        self.client.publish(topic, json.dumps(payload))
        logger.info(f"Created automation: {automation_name}")
        
        return True
    
    def subscribe_to_topic(self, topic, callback):
        """Subscribe to a custom MQTT topic with callback"""
        if topic not in self.callbacks:
            self.callbacks[topic] = []
            self.client.subscribe(topic)
        
        self.callbacks[topic].append(callback)
        logger.info(f"Subscribed to topic: {topic}")
    
    def get_energy_usage(self, device_id=None):
        """Get energy usage for device or all devices"""
        topic = "smarthome/energy/query"
        payload = {
            'device_id': device_id,
            'timestamp': datetime.now().isoformat()
        }
        
        self.client.publish(topic, json.dumps(payload))
        
        # In production, this would wait for response
        return True


class SmartHomeAutomation:
    """
    High-level smart home automation manager
    """
    
    def __init__(self, device_manager: IoTDeviceManager):
        self.device_manager = device_manager
        self.scenes = {}
        self.automations = {}
    
    def good_morning_scene(self):
        """Activate good morning scene"""
        lights = self.device_manager.get_devices('light')
        thermostats = self.device_manager.get_devices('thermostat')
        
        # Turn on lights gradually
        for device_id in lights:
            self.device_manager.turn_on(device_id)
            self.device_manager.set_brightness(device_id, 50)
        
        # Set comfortable temperature
        for device_id in thermostats:
            self.device_manager.set_temperature(device_id, 22)
        
        logger.info("Activated good morning scene")
    
    def good_night_scene(self):
        """Activate good night scene"""
        lights = self.device_manager.get_devices('light')
        locks = self.device_manager.get_devices('lock')
        thermostats = self.device_manager.get_devices('thermostat')
        
        # Turn off all lights
        for device_id in lights:
            self.device_manager.turn_off(device_id)
        
        # Lock all doors
        for device_id in locks:
            self.device_manager.lock(device_id)
        
        # Lower temperature
        for device_id in thermostats:
            self.device_manager.set_temperature(device_id, 18)
        
        logger.info("Activated good night scene")
    
    def away_mode(self):
        """Activate away mode"""
        lights = self.device_manager.get_devices('light')
        locks = self.device_manager.get_devices('lock')
        thermostats = self.device_manager.get_devices('thermostat')
        
        # Turn off all lights
        for device_id in lights:
            self.device_manager.turn_off(device_id)
        
        # Lock all doors
        for device_id in locks:
            self.device_manager.lock(device_id)
        
        # Set energy-saving temperature
        for device_id in thermostats:
            self.device_manager.set_temperature(device_id, 15)
        
        logger.info("Activated away mode")


# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize device manager
    device_manager = IoTDeviceManager(broker_host='localhost', broker_port=1883)
    device_manager.connect()
    
    # Initialize automation manager
    automation = SmartHomeAutomation(device_manager)
    
    # Wait for device discovery
    import time
    time.sleep(5)
    
    # List discovered devices
    devices = device_manager.get_devices()
    print(f"\nDiscovered {len(devices)} devices:")
    for device_id, device in devices.items():
        print(f"  - {device['name']} ({device['type']})")
    
    # Example: Control a light
    lights = device_manager.get_devices('light')
    if lights:
        light_id = list(lights.keys())[0]
        print(f"\nControlling light: {lights[light_id]['name']}")
        device_manager.turn_on(light_id)
        device_manager.set_brightness(light_id, 75)
        device_manager.set_color(light_id, '#FF5733')
    
    # Example: Activate scene
    print("\nActivating good morning scene...")
    automation.good_morning_scene()
    
    # Keep running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
        device_manager.disconnect()