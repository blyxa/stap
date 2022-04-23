# Stream Tap `stap`

is a command line tool for reading data from kafka. It supports the following serializers...
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

# show available methods
java -jar build/libs/stap-<version>.jar 

# list topics
java -jar build/libs/stap-<version>.jar localhost listTopics

# see more log
java -jar build/libs/stap-<version>.jar localhost listGroups -vvv

```