Simplaex Excercise
--------

The class for processing messages with Spark is:

`com.example.StreamingApp`

I created also a class to test just the computation in one batch:

`com.example.BatchApp`

To build the jar, open a command prompt in the directory SparkStreaming, which contains the file build.sbt, and run `sbt package`

The jar is generated inside the directory: `target/scala-2.11`

Test the application
--------

Run zookeeper

`zookeeper-server-start.bat config\zookeeper.properties`

Run Kafka Server

`kafka-server-start.bat config\server.properties`

Create a Kafka Topic with one partition

`kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic kafka-to-spark-streaming`

Send TCP messages received on port 9000 to the Kafka Topic

`nc -l -p 9000 | kafka-console-producer --broker-list localhost:9092 --topic kafka-to-spark-streaming`

Start the Producer

`java -jar producer.jar --tcp`


The job can be run in a Spark Cluster, with the command: `spark-submit --class com.example.StreamingApp`

To test, I used a Run Configuration in the development environment (Intellij), setting the environment variable `HADOOP_HOME` to `hadoop-3.1.2`, to run `com.example.StreamingApp` 

The output files are generated in the directory `output`

