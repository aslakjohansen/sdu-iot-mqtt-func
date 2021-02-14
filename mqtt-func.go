package main

import (
    "fmt"
    "encoding/json"
    "sync"
    "strings"
    "math"
    
    "github.com/eclipse/paho.mqtt.golang"
)

type Sample struct {
    Time  float64 `json:"time"`
    Value float64 `json:"value"`
}

var (
    brokers []string = []string{"tcp://127.0.0.1:1883"}
    pattern   string = "siggen/+/+"
    dispatch map[string]chan Sample = make(map[string]chan Sample)
    dispatch_mux sync.Mutex
    client mqtt.Client
)

func publish (topic string, channel chan Sample) {
    for sample := range channel {
        fmt.Println("Publishing to", topic)
        message, _ := json.Marshal(sample)
        client.Publish(topic, 1, false, message)
    }
}

// unit: g/m³
func calc_abs_hum (temp float64, rhum float64) float64 {
    return 6.112*math.Pow(math.E, (17.67*temp)/(temp+243.5))*rhum*2.1674/(273.15+temp)
}

func ahum (channel_temp chan Sample,
           channel_rhum chan Sample,
           channel_ahum chan Sample) {
    for {
        fmt.Println("a")
        temp_sample := <- channel_temp
        fmt.Println("b")
        rhum_sample := <- channel_rhum
        fmt.Println("c")
        
        temp := temp_sample.Value
        rhum := rhum_sample.Value
        ahum := calc_abs_hum(temp, rhum)
        fmt.Println(temp, rhum, ahum)
        
        channel_ahum <- Sample{temp_sample.Time, ahum}
    }
}

func dispatch_sample (client mqtt.Client, message mqtt.Message) {
    var topic string = message.Topic()
    var sample Sample
    var channel_temp chan Sample
    var channel_rhum chan Sample
    var channel_ahum chan Sample
    fmt.Println("dispatch enter")
    
    // preprocess topic
    tparts := strings.Split(topic, "/")
    modality := tparts[len(tparts)-1]
    
    // unmarshal
    err := json.Unmarshal(message.Payload(), &sample)
    if err!=nil {
        fmt.Println("Unable to unmarshal incoming sample:", err)
        return
    }
    
    // make sure that channel exists
    dispatch_mux.Lock()
    defer dispatch_mux.Unlock()
    channel, ok := dispatch[topic]
    if !ok {
        channel_temp = make(chan Sample, 2)
        channel_rhum = make(chan Sample, 2)
        channel_ahum = make(chan Sample, 2)
        
        // choose action
        switch modality {
        case "temp":
            channel = channel_temp
        case "rhum":
            channel = channel_rhum
        default:
            fmt.Println("dispatch abnormal exit")
            return
        }
        
        // define topic names
        topic_temp := "siggen/"+strings.Join(tparts[1:len(tparts)-1], "/")+"/temp"
        topic_rhum := "siggen/"+strings.Join(tparts[1:len(tparts)-1], "/")+"/rhum"
        topic_ahum := "func/"  +strings.Join(tparts[1:len(tparts)-1], "/")+"/ahum"
        
        fmt.Println(topic_temp+" + "+topic_rhum+" => "+topic_ahum)
        
        go publish(topic_ahum, channel_ahum)
        go ahum(channel_temp, channel_rhum, channel_ahum)
        
        dispatch[topic_temp] = channel_temp
        dispatch[topic_rhum] = channel_rhum
    }
    
    // queue channel
    channel <- sample
    fmt.Println("dispatch normal exit")
}

func mqtt_subscribe () {
    // configure options
    options := mqtt.NewClientOptions()
    for _, broker := range brokers {
      options.AddBroker(broker)
    }
    
    // start mqtt client
    client = mqtt.NewClient(options)
    if token := client.Connect(); token.Wait() && token.Error() != nil {
        panic(token.Error())
    }
    
    // set up subscription
    if token := client.Subscribe(pattern, 2, dispatch_sample); token.Wait() && token.Error() != nil {
        panic(token.Error())
    }
}

func main () {
    mqtt_subscribe()
    
    select{} // block forever
}
