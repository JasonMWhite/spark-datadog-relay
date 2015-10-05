# DatadogRelay
[![Circle CI](https://circleci.com/gh/JasonMWhite/spark-datadog-relay/tree/master.svg?style=svg)](https://circleci.com/gh/JasonMWhite/spark-datadog-relay/tree/master)

`DatadogRelay` is a `SparkListener` that consumes `SparkListenerEvent`s to JSON and sends data on to Datadog.

To compile, run:
```
mvn package
```

To connect it to Spark, add the appropriate flags to `spark-shell`. For example:
```
./bin/spark-shell --master local[4] --driver-class-path /path/to/repo/spark-datadog-relay/target/datadog-relay-0.0.1-SNAPSHOT.jar --conf spark.extraListeners=org.apache.spark.DatadogRelay
```

To authorize access to post to your Datadog account, you must provide the API key in your Spark configuration, using `spark.datadog.key`

h/t to https://github.com/hammerlab/spark-json-relay
