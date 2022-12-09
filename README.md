# STAP (Stream Tap)

is a command line tool for (among other functions) reading/writing data from kafka. It supports the following serializers...
* Avro
* String

`stap` requires a configuration file (HOCON) named stap.conf in the working directory. In the file you specify the kafka broker information.

```
"kafka" : {
    "my-staging-kafka" : {
        "brokers" : "stg-broker1:9092"
    },
    "mykafkaclustername" : {
        "brokers" : "broker1:9092,broker2:9092"
    }
}
```

# Example
```bash
# clone repo
git clone git@github.com:blyxa/stap.git
cd stap

# build
./gradlew build

# configure kafka broker endpoints.
vi stap.conf

# show available commands
./stap --help

# list topics
./stap topics -t localhost

# create topic
./stap createTopic -t localhost -n myavrotopic -p 2 -r 1 --configs retention.ms=40000,retention.bytes=30000

# write avro record
./stap writeAvro -t localhost -n myavrotopic -s example.avro.json -k mykey -v '{ "name" : "myname", "year":{"int": 1991}, "color":null }'

# read avro topic (default read starting from latest. this example start from oldest)
./stap readAvro -t localhost -n myavrotopic -s example.avro.json -p false

# delete topic
./stap deleteTopic -t localhost -n myavrotopic
```

# Data Generation Feature
There are times when you need to generate a continuous stream of test data into a kafka topic. STAP allows users to generate test data using python. Python is used only for the generation of the test data (as json string). STAP will initialize the kafka producer and call the python script to generate the test data and then write to the topic.
* example.avro.json represents the Avro schema we will use
* generator/python/generator.py contains code that will be called by STAP to generate the test data (json string)
* `./stap genAvro -t localhost -n myavrotopic -s example.avro.json` is the command for starting the process

## Example
```
# create a test topic
./stap createTopic -t localhost -n myavrotopic -p 2 -r 1 --configs retention.ms=60000,retention.bytes=10000000

# start the readAvro command
./stap readAvro -t localhost -n myavrotopic -s example.avro.json 

# in another terminal, start the genAvro command. 
# this command will generate 50 records at one record per second
./stap genAvro -t localhost -n myavrotopic -s example.avro.json -i 1000 -c 50

# you should see the records being printed in the readAvro terminal
```

