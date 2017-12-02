package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

func onMessageReceived(client MQTT.Client, message MQTT.Message) {
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
		if token := c.Subscribe("tellstick/raw", 0, onMessageReceived); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
	}

	client := MQTT.NewClient(connOpts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	} else {
		fmt.Printf("Connected to %s\n", *server)
	}

	for {
		time.Sleep(1 * time.Second)
	}
}
