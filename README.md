# Stream Tap `stap`

is a command line tool for (among other functions) reading/writing data from kafka. It supports the following serializers...
* Avro
* String

`stap` requires a configuration file (HOCON) named stap.conf in the working directory. In the file you specify the kafka information.

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

# add kafka cluster configuration in stap.conf else use the predefined localhost.
vi stap.conf

# show available commands
./stap --help

# list topics
./stap topics -t localhost

# create topic
./stap createTopic -t localhost -n myavrotopic -p 2 -r 1 --configs retention.ms=40000,retention.bytes=30000

# write avro record
./stap writeAvro --target localhost -n myavrotopic -s example.avro.json -k mykey -v '{ "name" : "myname", "year":{"int": 1991}, "color":null }'

# read avro topic (default read starting from latest. this example start from oldest)
./stap readAvro --target localhost -n myavrotopic -s example.avro.json -p false

# delete topic
./stap deleteTopic --target localhost -n myavrotopic
```