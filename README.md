# DatadogRelay
[![Circle CI](https://circleci.com/gh/JasonMWhite/spark-datadog-relay/tree/master.svg?style=svg)](https://circleci.com/gh/JasonMWhite/spark-datadog-relay/tree/master)

`DatadogRelay` is a `SparkListener` that consumes `SparkListenerEvent`s and sends data on to statsd. The server and port are hardcoded to `localhost:8125`.

To compile, run:
```
mvn package
```

To connect it to Spark, add the appropriate flags to `spark-shell`. For example:
```
./bin/spark-shell --master local[4] --driver-class-path /path/to/repo/spark-datadog-relay/target/datadog-relay-version.jar --conf spark.extraListeners=org.apache.spark.DatadogRelay
```

Alternatively, you can use the excellent (jitpack)[https://jitpack.io/#JasonMWhite/spark-datadog-relay]:
```
./bin/spark-shell --master local[4] --repositories https://jitpack.io --packages com.github.JasonMWhite:spark-datadog-relay:version --conf spark.extraListeners=org.apache.spark.DatadogRelay
```

This approach also works with pyspark.

h/t to https://github.com/hammerlab/spark-json-relay
