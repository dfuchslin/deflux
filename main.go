package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/dfuchslin/deflux/deconz"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	yaml "gopkg.in/yaml.v2"
)

// YmlFileName is the filename
const YmlFileName = "deflux.yml"

// Configuration holds data for Deconz and influxdb configuration
type Configuration struct {
	Deconz    deconz.Config
	Influxdb2 influxdb2ConfigProxy
}

func main() {
	config, err := loadConfiguration()
	if err != nil {
		log.Printf("no configuration could be found: %s", err)
		outputDefaultConfiguration()
		return
	}

	sensorChan, err := sensorEventChan(config.Deconz)
	if err != nil {
		panic(err)
	}

	log.Printf("Connected to deCONZ at %s", config.Deconz.Addr)

	influxdbv2 := influxdb2.NewClientWithOptions(config.Influxdb2.URL, config.Influxdb2.Token,
		influxdb2.DefaultOptions().SetBatchSize(config.Influxdb2.BatchSize))
	writeAPI := influxdbv2.WriteAPI(config.Influxdb2.Org, config.Influxdb2.Bucket)

	for {

		select {
		case sensorEvent := <-sensorChan:
			tags, fields, err := sensorEvent.Timeseries()
			if err != nil {
				log.Printf("not adding event to influx batch: %s", err)
				continue
			}

			writeAPI.WritePoint(influxdb2.NewPoint(
				fmt.Sprintf("deflux_%s", sensorEvent.Sensor.Type),
				tags,
				fields,
				time.Now(), // TODO: we should use the time associated with the event...
			))

		}
	}
}

func sensorEventChan(c deconz.Config) (chan *deconz.SensorEvent, error) {
	// get an event reader from the API
	d := deconz.API{Config: c}
	reader, err := d.EventReader()
	if err != nil {
		return nil, err
	}

	// Dial the reader
	err = reader.Dial()
	if err != nil {
		return nil, err
	}

	// create a new reader, embedding the event reader
	sensorEventReader := d.SensorEventReader(reader)
	channel := make(chan *deconz.SensorEvent)
	// start it, it starts its own thread
	sensorEventReader.Start(channel)
	// return the channel
	return channel, nil
}

func loadConfiguration() (*Configuration, error) {
	data, err := readConfiguration()
	if err != nil {
		return nil, fmt.Errorf("could not read configuration: %s", err)
	}

	var config Configuration
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("could not parse configuration: %s", err)
	}
	return &config, nil
}

// readConfiguration tries to read pwd/deflux.yml or /etc/deflux.yml
func readConfiguration() ([]byte, error) {
	// first try to load ${pwd}/deflux.yml
	pwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("unable to get current work directory: %s", err)
	}

	pwdPath := path.Join(pwd, YmlFileName)
	data, pwdErr := ioutil.ReadFile(pwdPath)
	if pwdErr == nil {
		log.Printf("Using configuration %s", pwdPath)
		return data, nil
	}

	// if we reached this code, we where unable to read a "local" Configuration
	// try from /etc/deflux.yml
	etcPath := path.Join("/etc", YmlFileName)
	data, etcErr := ioutil.ReadFile(etcPath)
	if etcErr != nil {
		return nil, fmt.Errorf("\n%s\n%s", pwdErr, etcErr)
	}

	log.Printf("Using configuration %s", etcPath)
	return data, nil
}

// influxdbConfigProxy proxies the influxdbv2 config into a yml capable
// struct, its only used for encoding to yml as the yml package
// have no problem skipping the Proxy field when decoding
type influxdb2ConfigProxy struct {
	URL       string
	Org       string
	Token     string
	Bucket    string
	BatchSize uint
}

func outputDefaultConfiguration() {

	c := defaultConfiguration()

	// try to pair with deconz
	u, err := url.Parse(c.Deconz.Addr)
	if err == nil {
		apikey, err := deconz.Pair(*u)
		if err != nil {
			log.Printf("unable to pair with deconz: %s, please fill out APIKey manually", err)
		}
		c.Deconz.APIKey = string(apikey)
	}

	// we need to use a proxy struct to encode yml as the influxdb client configuration struct
	// includes a Proxy: func() field that the yml encoder cannot handle
	yml, err := yaml.Marshal(struct {
		Deconz    deconz.Config
		Influxdb2 influxdb2ConfigProxy
	}{
		Deconz: c.Deconz,
		Influxdb2: influxdb2ConfigProxy{
			URL:       c.Influxdb2.URL,
			Org:       c.Influxdb2.Org,
			Token:     c.Influxdb2.Token,
			Bucket:    c.Influxdb2.Bucket,
			BatchSize: c.Influxdb2.BatchSize,
		},
	})
	if err != nil {
		log.Fatalf("unable to generate default configuration: %s", err)
	}

	log.Printf("Outputting default configuration, save this to /etc/deflux.yml")
	// to stdout
	fmt.Print(string(yml))
}

func defaultConfiguration() *Configuration {
	// this is the default configuration
	c := Configuration{
		Deconz: deconz.Config{
			Addr:   "http://127.0.0.1:8080/",
			APIKey: "change me",
		},
		Influxdb2: influxdb2ConfigProxy{
			URL:       "http://127.0.0.1:8086/",
			Org:       "change me",
			Token:     "change me",
			Bucket:    "change me",
			BatchSize: 20,
		},
	}

	// lets see if we are able to discover a gateway, and overwrite parts of the
	// default congfiguration
	discovered, err := deconz.Discover()
	if err != nil {
		log.Printf("discovery of deconz gateway failed: %s, please fill configuration manually..", err)
		return &c
	}

	// TODO: discover is actually a slice of multiple discovered gateways,
	// but for now we use only the first available
	deconz := discovered[0]
	addr := url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%d", deconz.InternalIPAddress, deconz.InternalPort),
		Path:   "/api",
	}
	c.Deconz.Addr = addr.String()

	return &c
}
