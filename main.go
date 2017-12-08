package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/gosimple/slug"
	yaml "gopkg.in/yaml.v2"
)

type Config struct {
	Thermometers []struct {
		Name     string
		Topic    string
		Humidity bool
	}
	Buttons []struct {
		Name  string
		Topic string
	}
	Lights []struct {
		Name  string
		Topic string
	}
}

var config Config

type sensorInfo struct {
	StateTopic string `json:"state_topic"`
	Name       string `json:"name"`
	Unit       string `json:"unit_of_measurement"`
}

type binarySensorInfo struct {
	Name       string `json:"name"`
	StateTopic string `json:"state_topic"`
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

var changeLight = make(chan changeLightReq)
var rediscover = make(chan bool)

func homeAssistantDiscoverer(client MQTT.Client) {
	for {
		select {
		case <-rediscover:
			fmt.Println("Publishing data to HA MQTT Discoverer")
			updateHomeAssistantThermometers(client, config)
			updateHomeAssistantButtons(client, config)
			updateHomeAssistantLights(client, config)
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

func updateHomeAssistantThermometers(client MQTT.Client, config Config) {
	for _, thermometer := range config.Thermometers {
		slug := slug.Make(thermometer.Name)
		hassSlug := strings.Replace(slug, "-", "_", -1)
		tempInfo := sensorInfo{
			StateTopic: "thermometers/" + slug + "/value",
			Name:       thermometer.Name,
			Unit:       "°C",
		}
		tempJSON, _ := json.Marshal(&tempInfo)
		client.Publish("homeassistant/sensor/"+hassSlug+"_temperature/config", 0, false, tempJSON)
		if thermometer.Humidity {
			tempInfo := sensorInfo{
				StateTopic: "hygrometers/" + slug + "/value",
				Name:       thermometer.Name + " H",
				Unit:       "%",
			}
			tempJSON, _ := json.Marshal(&tempInfo)
			client.Publish("homeassistant/sensor/"+hassSlug+"_humidity/config", 0, false, tempJSON)
		}
	}
}

func updateHomeAssistantButtons(client MQTT.Client, config Config) {
	for _, button := range config.Buttons {
		slug := slug.Make(button.Name)
		hassSlug := strings.Replace(slug, "-", "_", -1)
		info := binarySensorInfo{
			Name:       button.Name,
			StateTopic: "buttons/" + slug + "/value",
		}
		j, _ := json.Marshal(&info)
		client.Publish("homeassistant/binary_sensor/"+hassSlug+"/config", 0, false, j)
	}
}

func updateHomeAssistantLights(client MQTT.Client, config Config) {
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

func updateThermometers(client MQTT.Client, config Config) {
	for _, thermometer := range config.Thermometers {
		slug := slug.Make(thermometer.Name)
		client.Publish("thermometers/"+slug+"/name", 0, true, []byte(thermometer.Name))
		client.Publish("thermometers/"+slug+"/unit", 0, true, []byte("celsius"))
		if thermometer.Humidity {
			client.Publish("hygrometers/"+slug+"/name", 0, true, []byte(thermometer.Name))
			client.Publish("hygrometers/"+slug+"/unit", 0, true, []byte("percent"))
		}
	}
}

func updateButtons(client MQTT.Client, config Config) {
	for _, button := range config.Buttons {
		slug := slug.Make(button.Name)
		client.Publish("buttons/"+slug+"/name", 0, true, []byte(button.Name))
	}
}

func updateLights(client MQTT.Client, config Config) {
	for _, light := range config.Lights {
		slug := slug.Make(light.Name)
		client.Publish("lights/"+slug+"/name", 0, true, []byte(light.Name))
	}
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

func onConfig(client MQTT.Client, message MQTT.Message) {
	var newConfig Config
	err := yaml.Unmarshal(message.Payload(), &newConfig)
	if err != nil {
		log.Printf("Error parsing config: %v", err)
	}
	fmt.Println("Got config update")

	updateThermometers(client, newConfig)
	updateButtons(client, newConfig)
	updateLights(client, newConfig)

	config = newConfig
	rediscover <- true
}

func handleThermometer(client MQTT.Client, topic string, values map[string]string) {
	for _, thermometer := range config.Thermometers {
		if thermometer.Topic == topic {
			slug := slug.Make(thermometer.Name)
			client.Publish("thermometers/"+slug+"/value", 0, true, []byte(values["temp"]))
			if thermometer.Humidity {
				client.Publish("hygrometers/"+slug+"/value", 0, true, []byte(values["humidity"]))
			}
		}
	}
}

func handleButtons(client MQTT.Client, topic string, values map[string]string) {
	for _, button := range config.Buttons {
		if button.Topic == topic {
			slug := slug.Make(button.Name)
			if values["method"] == "turnon" {
				client.Publish("buttons/"+slug+"/value", 0, true, []byte("ON"))
			} else if values["method"] == "turnoff" {
				client.Publish("buttons/"+slug+"/value", 0, true, []byte("OFF"))
			}
		}
	}
}

func handleRaw(client MQTT.Client, message MQTT.Message) {
	values := make(map[string]string)
	for _, kvpair := range strings.Split(string(message.Payload()), ";") {
		kv := strings.SplitN(kvpair, ":", 2)
		if len(kv) == 2 {
			values[kv[0]] = kv[1]
		}
	}

	if _, ok := values["protocol"]; ok {
		delete(values, "protocol")
	}
	if _, ok := values["group"]; ok {
		delete(values, "group")
	}

	topic := "tellstick/"
	if class, ok := values["class"]; ok {
		topic = topic + class + "/"
		delete(values, "class")
	}
	if model, ok := values["model"]; ok {
		topic = topic + model + "/"
		delete(values, "model")
	}
	if id, ok := values["id"]; ok {
		topic = topic + id + "/"
		delete(values, "id")
	}
	if house, ok := values["house"]; ok {
		topic = topic + house + "/"
		delete(values, "house")
	}
	if unit, ok := values["unit"]; ok {
		topic = topic + unit + "/"
		delete(values, "unit")
	}

	// Remove trailing slash of topic
	topic = topic[0 : len(topic)-1]

	payload, err := json.Marshal(values)
	if err == nil {
		fmt.Printf("Topic: %s, Payload: %s\n", topic, string(payload))
		if token := client.Publish(topic, byte(0), false, payload); token.Wait() && token.Error() != nil {
			fmt.Printf("PUBLISH ERROR: %v", token.Error())
		}
		handleThermometer(client, topic, values)
		handleButtons(client, topic, values)
	}
}

var i int64

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
	clientid := flag.String("clientid", hostname+strconv.Itoa(time.Now().Second()), "A clientid for the connection")
	username := flag.String("username", "", "A username to authenticate to the MQTT server")
	password := flag.String("password", "", "Password to match username")
	flag.Parse()

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
		if token := c.Subscribe("house/config", 0, onConfig); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
		if token := c.Subscribe("tellstick/raw", 0, handleRaw); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
		if token := c.Subscribe("lights/+/command", 0, onLight); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
		if token := c.Subscribe("homeassistant/started", 0, onHomeAssistantStarted); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
	}

	client := MQTT.NewClient(connOpts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	} else {
		fmt.Printf("Connected to %s\n", *server)
	}

	go lightMonitor(client)
	go homeAssistantDiscoverer(client)

	for {
		time.Sleep(1 * time.Second)
	}
}
