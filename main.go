package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/gosimple/slug"
	yaml "gopkg.in/yaml.v2"

	influxdb "github.com/influxdata/influxdb/client/v2"
)

type DeconzMessage struct {
	Typ      string                 `json:"t"`
	Event    string                 `json:"e"`
	Resource string                 `json:"r"`
	ID       string                 `json:"id"`
	State    map[string]interface{} `json:"state"`
	Config   map[string]interface{} `json:"config"`
}

type SensorEntity struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Sensor struct {
	Name     string            `json:"name"`
	Protocol string            `json:"protocol"`
	Model    string            `json:"model"`
	Typ      string            `json:"type"`
	Entities []SensorEntity    `json:"entities"`
	Labels   map[string]string `json:"labels"`
}

type Config struct {
	Sensors []Sensor
	Lights  []struct {
		Name  string `json:"name"`
		Topic string `json:"topic"`
		Area  string `json:"area"`
		Floor string `json:"floor,omitempty"`
	}
}

var config Config

type sensorInfo struct {
	StateTopic string `json:"state_topic"`
	Name       string `json:"name"`
	Unit       string `json:"unit_of_measurement"`
	Icon       string `json:"icon,omitempty"`
	UniqueID   string `json:"unique_id"`
}

type binarySensorInfo struct {
	Name        string `json:"name"`
	StateTopic  string `json:"state_topic"`
	DeviceClass string `json:"device_class,omitempty"`
}

type lightInfo struct {
	CommandTopic string `json:"command_topic"`
	Name         string `json:"name"`
	StateTopic   string `json:"state_topic"`
}

type changeLightReq struct {
	ID    string
	Value bool
}

type sensorValue struct {
	sensor *Sensor
	entity string
	value  float64
}

const medianFilterSamples = 9

var medianSamples = make(map[string][]float64)
var changeLight = make(chan changeLightReq)
var rediscover = make(chan bool)

var sensor2Influxchan = make(chan sensorValue)
var sensor2MQTTchan = make(chan sensorValue)

func homeAssistantDiscoverer(client MQTT.Client) {
	for {
		select {
		case <-rediscover:
			fmt.Println("Publishing data to HA MQTT Discoverer")
			updateHomeAssistantSensors(client)
			updateHomeAssistantLights(client)
		}
	}
}

func influxdbReporter(address, username, password string) {
	for v := range sensor2Influxchan {

		if len(v.sensor.Labels) == 0 {
			continue
		}

		c, err := influxdb.NewHTTPClient(influxdb.HTTPConfig{
			Addr:     address,
			Username: username,
			Password: password,
		})
		if err != nil {
			log.Fatal(err)
		}
		defer c.Close()

		bp, err := influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
			Database:  "sensors",
			Precision: "s",
		})
		if err != nil {
			log.Fatal(err)
		}

		tags := make(map[string]string)
		tags["protocol"] = v.sensor.Protocol
		tags["model"] = v.sensor.Model
		tags["type"] = v.sensor.Typ
		tags["name"] = slug.Make(v.sensor.Name)
		for k, v := range v.sensor.Labels {
			tags[k] = v
		}
		fields := map[string]interface{}{
			"value": v.value,
		}

		fmt.Printf("Influx <- %s = %s\n", v.entity, strconv.FormatFloat(v.value, 'f', -1, 64))
		pt, err := influxdb.NewPoint(v.entity, tags, fields, time.Now())
		if err != nil {
			log.Fatal(err)
		}
		bp.AddPoint(pt)

		if err := c.Write(bp); err != nil {
			log.Fatal(err)
		}
	}
}

func lightMonitor(client MQTT.Client) {
	states := make(map[string]bool)

	t := time.NewTicker(30 * time.Second)
	for {
		select {
		case m := <-changeLight:
			fmt.Printf("Change Light: %s -> %v\n", m.ID, m.Value)
			for _, light := range config.Lights {
				slug := slug.Make(light.Name)
				var payload []byte
				if m.Value {
					payload = []byte("ON")
				} else {
					payload = []byte("OFF")
				}
				if m.ID == slug {
					fmt.Printf("Posting on topic %s for %s\n", light.Topic, light.Name)
					states[light.Topic] = m.Value
					client.Publish(light.Topic, 0, false, payload)
					client.Publish("lights/"+slug+"/state", 0, true, payload)
					break
				}
			}
		case <-t.C:
			// Pick a random every iteration
			for topic, value := range states {
				var payload []byte
				if value {
					payload = []byte("ON")
				} else {
					payload = []byte("OFF")
				}
				client.Publish(topic, 0, false, payload)
				break
			}
		}

	}
}

func updateHomeAssistantSensors(client MQTT.Client) {
	addSensor := func(id string, sensor interface{}) {
		tempJSON, _ := json.Marshal(&sensor)
		client.Publish("homeassistant/sensor/"+id+"/config", 0, false, tempJSON)
	}
	for _, sensor := range config.Sensors {
		slug := slug.Make(sensor.Name)
		hassSlug := strings.Replace(slug, "-", "_", -1)

		if sensor.Model == "temperaturehumidity" {
			addSensor(hassSlug+"_temperature", sensorInfo{
				StateTopic: "sensors/" + slug + "/temperature",
				Name:       sensor.Name + " temperature",
				Unit:       "°C",
				Icon:       "mdi:thermometer",
				UniqueID:   "sensor-" + slug + "_temp",
			})
			addSensor(hassSlug+"_humidity", sensorInfo{
				StateTopic: "sensors/" + slug + "/humidity",
				Name:       sensor.Name + " humidity",
				Unit:       "%",
				Icon:       "mdi:water-percent",
				UniqueID:   "sensor-" + slug + "_humidity",
			})
		} else if sensor.Model == "selflearning" {
			// TODO: Must verify door sensor
			// for _, entity := range sensor.Entities {
			// 	addSensor(hassSlug+"_switch", binarySensorInfo{
			// 		StateTopic:  "sensors/" + slug + "/" + entity.Name,
			// 		Name:        sensor.Name + " " + entity.Name,
			// 		DeviceClass: "opening",
			// 	})
			// }
		} else if sensor.Model == "aqara-thermometer" {
			addSensor(hassSlug+"_temperature", sensorInfo{
				StateTopic: "sensors/" + slug + "/temperature",
				Name:       sensor.Name + " temperature",
				Unit:       "°C",
				Icon:       "mdi:thermometer",
				UniqueID:   "sensor-" + slug + "_temp",
			})
			addSensor(hassSlug+"_humidity", sensorInfo{
				StateTopic: "sensors/" + slug + "/humidity",
				Name:       sensor.Name + " humidity",
				Unit:       "%",
				Icon:       "mdi:water-percent",
				UniqueID:   "sensor-" + slug + "_humidity",
			})
			addSensor(hassSlug+"_pressure", sensorInfo{
				StateTopic: "sensors/" + slug + "/pressure",
				Name:       sensor.Name + " pressure",
				Unit:       "Pa",
				Icon:       "mdi:gauge",
				UniqueID:   "sensor-" + slug + "_pressure",
			})
		} else if sensor.Model == "aqara-motion" {
			addSensor(hassSlug+"_presence", binarySensorInfo{
				StateTopic:  "sensors/" + slug + "/presence",
				Name:        sensor.Name + " presence",
				DeviceClass: "presence",
			})
			addSensor(hassSlug+"_lux", sensorInfo{
				StateTopic: "sensors/" + slug + "/lux",
				Name:       sensor.Name + " lux",
				Unit:       "lux",
				Icon:       "mdi:lightbulb-on",
				UniqueID:   "sensor-" + slug + "_lux",
			})
		} else if sensor.Model == "aqara-flood" {
			addSensor(hassSlug+"_water", binarySensorInfo{
				StateTopic:  "sensors/" + slug + "/water",
				Name:        sensor.Name + " water",
				DeviceClass: "moisture",
			})
		} else if sensor.Model == "aqara-button" {
			// TODO: Must support click/double-click etc
			// addSensor(hassSlug+"_button", binarySensorInfo{
			// 	StateTopic:  "sensors/" + slug + "/button",
			// 	Name:        sensor.Name + " button",
			// 	DeviceClass: "moisture",
			// })
		}
	}
}

func updateHomeAssistantLights(client MQTT.Client) {
	for _, light := range config.Lights {
		slug := slug.Make(light.Name)
		hassSlug := strings.Replace(slug, "-", "_", -1)
		info := lightInfo{
			CommandTopic: "lights/" + slug + "/command",
			Name:         light.Name,
			StateTopic:   "lights/" + slug + "/state",
		}
		j, _ := json.Marshal(&info)
		client.Publish("homeassistant/light/"+hassSlug+"/config", 0, false, j)
	}
}

func makeKvString(labels map[string]string) string {
	b, _ := json.Marshal(labels)
	return string(b)
}

func onLight(client MQTT.Client, message MQTT.Message) {
	parts := strings.SplitN(message.Topic(), "/", 3)
	if string(message.Payload()) == "ON" {
		changeLight <- changeLightReq{
			ID:    parts[1],
			Value: true,
		}
	} else if string(message.Payload()) == "OFF" {
		changeLight <- changeLightReq{
			ID:    parts[1],
			Value: false,
		}
	}
}

func onHomeAssistantStarted(client MQTT.Client, message MQTT.Message) {
	if string(message.Payload()) == "on" {
		rediscover <- true
	}
}

var allowedTypes = map[string]bool{
	"pir":          true,
	"door-sensor":  true,
	"thermometer":  true,
	"flood-sensor": true,
}

func readConfig(filename string) error {
	log.Printf("Reading config from %s\n", filename)
	contents, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Printf("Error reading config %s: %v", filename, err)
		return err
	}
	err = yaml.Unmarshal(contents, &config)
	if err != nil {
		log.Printf("Error parsing config: %v", err)
		return err
	}
	fmt.Printf("Found Sensors:\n")
	for _, sensor := range config.Sensors {
		fmt.Printf(" - %s (%s::%s::%s)\n", sensor.Name, sensor.Protocol, sensor.Typ, sensor.Model)
		if _, found := allowedTypes[sensor.Typ]; !found {
			panic("Invalid type")
		}
	}

	return nil
}

func median(input []float64) float64 {
	c := make([]float64, len(input))
	copy(c, input)
	sort.Float64s(c)

	l := len(c)
	var ret float64
	if l%2 == 0 {
		ret = 0.5 * (c[l/2-1] + c[l/2])
	} else {
		ret = c[l/2]
	}

	return ret
}

func medianFilterPublish(client MQTT.Client, topic string, value float64) {
	new := append(medianSamples[topic], value)
	if len(new) > medianFilterSamples {
		new = new[len(new)-medianFilterSamples:]
	}

	if len(new) == medianFilterSamples {
		value := median(new)
		s := fmt.Sprint(value)
		client.Publish(topic, 0, true, []byte(s))
		fmt.Printf("<- %s = %v = %s\n", topic, new, s)
	} else {
		fmt.Printf("<- %s = %v = too few\n", topic, new)
	}

	medianSamples[topic] = new
}

func sendParsedTellstick(client MQTT.Client, values map[string]string) {
	state := make(map[string]string)
	for k, v := range values {
		state[k] = v
	}

	if _, ok := state["protocol"]; ok {
		delete(state, "protocol")
	}
	if _, ok := state["group"]; ok {
		delete(state, "group")
	}

	topic := "tellstick/"
	if class, ok := state["class"]; ok {
		topic = topic + class + "/"
		delete(state, "class")
	}
	if model, ok := state["model"]; ok {
		topic = topic + model + "/"
		delete(state, "model")
	}
	if id, ok := state["id"]; ok {
		topic = topic + id + "/"
		delete(state, "id")
	}
	if house, ok := state["house"]; ok {
		topic = topic + house + "/"
		delete(state, "house")
	}
	if unit, ok := state["unit"]; ok {
		topic = topic + unit + "/"
		delete(state, "unit")
	}

	// Remove trailing slash of topic
	topic = topic[0 : len(topic)-1]

	payload, err := json.Marshal(state)
	if err == nil {
		publish(client, topic+"/state", false, payload)
	}
	for k, v := range state {
		publishString(client, topic+"/"+k, false, v)
	}
}

func handleRawTellstick(client MQTT.Client, message MQTT.Message) {
	fmt.Printf("-> %s = %s\n", message.Topic(), string(message.Payload()))
	values := make(map[string]string)
	for _, kvpair := range strings.Split(string(message.Payload()), ";") {
		kv := strings.SplitN(kvpair, ":", 2)
		if len(kv) == 2 {
			values[kv[0]] = kv[1]
		}
	}

	sendParsedTellstick(client, values)

	findSensor := func(typ, entityID string) (Sensor, SensorEntity, bool) {
		for _, sensor := range config.Sensors {
			if sensor.Protocol == "tellstick" && sensor.Model == typ {
				for _, entity := range sensor.Entities {
					if entity.ID == entityID {
						return sensor, entity, true
					}
				}
			}
		}
		return Sensor{}, SensorEntity{}, false
	}

	// tellstick/selflearning
	if values["class"] == "sensor" && values["protocol"] == "arctech" && values["model"] == "selflearning" {
		id := values["house"] + "/" + values["unit"]
		if sensor, entity, found := findSensor("selflearning", id); found {
			slug := slug.Make(sensor.Name)
			if values["method"] == "turnon" {
				publishString(client, "sensors/"+slug+"/"+entity.Name, false, "1")
			} else if values["method"] == "turnoff" {
				publishString(client, "sensors/"+slug+"/"+entity.Name, false, "0")
			}
		}
	}

	// tellstick/temperaturehumidity
	if values["class"] == "sensor" && values["protocol"] == "fineoffset" && values["model"] == "temperaturehumidity" {
		id := values["id"]
		if sensor, _, found := findSensor("temperaturehumidity", id); found {
			if f, err := strconv.ParseFloat(values["temp"], 64); err == nil {
				publishSensor(&sensor, "temperature", f)
			}
			if f, err := strconv.ParseFloat(values["humidity"], 64); err == nil {
				publishSensor(&sensor, "humidity", f)
			}
		}
	}
}

func handleRawSPC(client MQTT.Client, message MQTT.Message) {
	fmt.Printf("-> %s = %s\n", message.Topic(), string(message.Payload()))
	var data struct {
		UpdateTime int64  `json:"update_time"`
		Status     string `json:"status"`
	}
	json.Unmarshal(message.Payload(), &data)
	parts := strings.Split(message.Topic(), "/")

	findSensor := func(entityID string) *Sensor {
		for _, sensor := range config.Sensors {
			if sensor.Protocol == "SPC" {
				for _, entity := range sensor.Entities {
					if entity.ID == entityID {
						return &sensor
					}
				}
			}
		}
		return nil
	}

	if sensor := findSensor(parts[1]); sensor != nil {
		if sensor.Typ == "pir" {
			if data.Status == "open" {
				publishSensor(sensor, "presence", 1)
			} else if data.Status == "closed" {
				publishSensor(sensor, "presence", 0)
			}
		} else if sensor.Typ == "door-switch" {
			if data.Status == "open" {
				publishSensor(sensor, "open", 1)
			} else if data.Status == "closed" {
				publishSensor(sensor, "open", 0)
			}
		}
	}
}

func publish(client MQTT.Client, topic string, retained bool, payload []byte) {
	fmt.Printf("<- %s = %s\n", topic, string(payload))
	if token := client.Publish(topic, 0, retained, payload); token.Wait() && token.Error() != nil {
		fmt.Printf("PUBLISH ERROR: %v", token.Error())
	}
}

func publishString(client MQTT.Client, topic string, retained bool, payload string) {
	publish(client, topic, retained, []byte(payload))
}

func sendParsedDeconz(client MQTT.Client, msg DeconzMessage) {
	topic := "deconz/" + msg.Resource + "/" + msg.ID + "/" + msg.Typ + "/" + msg.Event

	if msg.State != nil {
		payload, err := json.Marshal(msg.State)
		if err == nil {
			publish(client, topic+"/state/json", true, payload)
		}
		for k, v := range msg.State {
			publishString(client, topic+"/state/"+k, true, fmt.Sprintf("%v", v))
		}
	}
	if msg.Config != nil {
		payload, err := json.Marshal(msg.Config)
		if err == nil {
			publish(client, topic+"/config/json", true, payload)
		}
		for k, v := range msg.Config {
			publishString(client, topic+"/config/"+k, true, fmt.Sprintf("%v", v))
		}
	}
}

func bool2int(b bool) int {
	if b {
		return 1
	}
	return 0
}

func publishSensor(sensor *Sensor, entity string, v float64) {
	sensor2MQTTchan <- sensorValue{sensor, entity, v}
	sensor2Influxchan <- sensorValue{sensor, entity, v}
}

func handleAqaraMotion(client MQTT.Client, sensor Sensor, entity SensorEntity, msg DeconzMessage) {
	if msg.State != nil {
		if presence, ok := msg.State["presence"].(bool); ok {
			publishSensor(&sensor, "presence", float64(bool2int(presence)))
		}
		if lux, ok := msg.State["lux"].(float64); ok {
			publishSensor(&sensor, "lux", lux)
		}
		if lightLevel, ok := msg.State["lightlevel"].(float64); ok {
			publishSensor(&sensor, "lightlevel", lightLevel)
		}
	}
	if msg.Config != nil {
		if battery, ok := msg.Config["battery"].(float64); ok {
			publishSensor(&sensor, "battery", battery)
		}
		// The temperature is not very reliable. Skipping.
		// if temperature, ok := msg.Config["temperature"].(float64); ok {
		// 	publishSensor(&sensor, "temperature", temperature/100.0)
		// }
	}
}

func handleAqaraThermometer(client MQTT.Client, sensor Sensor, entity SensorEntity, msg DeconzMessage) {
	if msg.State != nil {
		if temperature, ok := msg.State["temperature"].(float64); ok {
			publishSensor(&sensor, "temperature", temperature/100.0)
		}
		if pressure, ok := msg.State["pressure"].(float64); ok {
			publishSensor(&sensor, "pressure", pressure/10.0)
		}
		if humidity, ok := msg.State["humidity"].(float64); ok {
			publishSensor(&sensor, "humidity", humidity/100.0)
		}
	}
	if msg.Config != nil {
		if battery, ok := msg.Config["battery"].(float64); ok {
			publishSensor(&sensor, "battery", battery)
		}
	}
}

func handleAqaraFlood(client MQTT.Client, sensor Sensor, entity SensorEntity, msg DeconzMessage) {
	if msg.State != nil {
		if water, ok := msg.State["water"].(bool); ok {
			publishSensor(&sensor, "water", float64(bool2int(water)))
		}
	}
	if msg.Config != nil {
		if battery, ok := msg.Config["battery"].(float64); ok {
			publishSensor(&sensor, "battery", battery)
		}
		// The temperature is not very reliable. Skipping.
		// if temperature, ok := msg.Config["temperature"].(float64); ok {
		// 	publishSensor(&sensor, "temperature", temperature/100.0)
		// }
	}
}
func handleAqaraButton(client MQTT.Client, sensor Sensor, entity SensorEntity, msg DeconzMessage) {
	topic := "sensors/" + slug.Make(sensor.Name)
	if msg.State != nil {
		if code, ok := msg.State["buttonevent"].(float64); ok {
			event := ""
			if code == 1001 {
				event = "long_press"
			} else if code == 1002 {
				event = "click"
			} else if code == 1003 {
				event = "long_release"
			} else if code == 1004 {
				event = "double-click"
			} else if code == 1007 {
				event = "motion"
			}
			if event != "" {
				publishString(client, topic+"/event", false, event)
			}
		}
	}
	if msg.Config != nil {
		if battery, ok := msg.Config["battery"].(float64); ok {
			publishSensor(&sensor, "battery", battery)
		}
		// The temperature is not very reliable. Skipping.
		// if temperature, ok := msg.Config["temperature"].(float64); ok {
		// 	publishSensor(&sensor, "temperature", temperature/100.0)
		// }
	}
}

func handleRawDeconz(client MQTT.Client, message MQTT.Message) {
	fmt.Printf("-> %s = %s\n", message.Topic(), string(message.Payload()))
	var msg DeconzMessage
	err := json.Unmarshal(message.Payload(), &msg)
	if err != nil {
		fmt.Printf("Failed to parse payload: %v", err)
		return
	}

	sendParsedDeconz(client, msg)

	findSensor := func(id string) (Sensor, SensorEntity, bool) {
		for _, sensor := range config.Sensors {
			if sensor.Protocol == "zigbee" {
				for _, entity := range sensor.Entities {
					if entity.ID == id {
						return sensor, entity, true
					}
				}
			}
		}
		return Sensor{}, SensorEntity{}, false
	}

	if sensor, entity, found := findSensor(msg.ID); found {
		if sensor.Model == "aqara-motion" {
			handleAqaraMotion(client, sensor, entity, msg)
		} else if sensor.Model == "aqara-thermometer" {
			handleAqaraThermometer(client, sensor, entity, msg)
		} else if sensor.Model == "aqara-flood" {
			handleAqaraFlood(client, sensor, entity, msg)
		} else if sensor.Model == "aqara-button" {
			handleAqaraButton(client, sensor, entity, msg)
		}
	}
}

var i int64

func sensorReporter(client MQTT.Client) {
	for v := range sensor2MQTTchan {
		topic := "sensors/" + slug.Make(v.sensor.Name) + "/" + v.entity
		publish(client, topic, true, []byte(strconv.FormatFloat(v.value, 'f', -1, 64)))
	}
}

func main() {
	//MQTT.DEBUG = log.New(os.Stdout, "", 0)
	//MQTT.ERROR = log.New(os.Stdout, "", 0)
	c := make(chan os.Signal, 1)
	i = 0
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("signal received, exiting")
		os.Exit(0)
	}()

	hostname, _ := os.Hostname()

	server := flag.String("server", "tcp://127.0.0.1:1883", "The full url of the MQTT server to connect to ex: tcp://127.0.0.1:1883")
	influxServer := flag.String("influxdb", "http://localhost:8086", "The full url of the influxdb server")
	clientid := flag.String("clientid", hostname+strconv.Itoa(time.Now().Second()), "A clientid for the connection")
	username := flag.String("username", "", "A username to authenticate to the MQTT server")
	password := flag.String("password", "", "Password to match username")
	configFilename := flag.String("config", "/config/config.yaml", "Config file")
	flag.Parse()

	err := readConfig(*configFilename)
	if err != nil {
		panic(err)
	}

	connOpts := &MQTT.ClientOptions{
		ClientID:             *clientid,
		CleanSession:         true,
		Username:             *username,
		Password:             *password,
		MaxReconnectInterval: 1 * time.Second,
		KeepAlive:            int64(30 * time.Second),
		TLSConfig:            tls.Config{InsecureSkipVerify: true, ClientAuth: tls.NoClientCert},
	}
	connOpts.AddBroker(*server)
	connOpts.OnConnect = func(c MQTT.Client) {
		if token := c.Subscribe("tellstick/raw", 0, handleRawTellstick); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
		if token := c.Subscribe("deconz/raw", 0, handleRawDeconz); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
		if token := c.Subscribe("lights/+/command", 0, onLight); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
		if token := c.Subscribe("homeassistant/started", 0, onHomeAssistantStarted); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
		if token := c.Subscribe("SPC/#", 0, handleRawSPC); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
	}

	connOpts.OnConnectionLost = func(c MQTT.Client, err error) {
		panic(fmt.Sprintf("Disconnected from MQTT server: %v", err))
	}

	client := MQTT.NewClient(connOpts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	} else {
		fmt.Printf("Connected to %s\n", *server)

		go lightMonitor(client)
		go homeAssistantDiscoverer(client)
		go influxdbReporter(*influxServer, "house", "house")
		go sensorReporter(client)

		rediscover <- true
	}

	for {
		time.Sleep(1 * time.Second)
	}
}
