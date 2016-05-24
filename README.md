#Bootcamp Spark Streaming Reference Project

In order to run this demo, It is assumed that you have the following installed and available on your local system.

  1. Datastax Enterprise 5.0.x
  2. Apache Kafka 0.9.0.1, I used the Scala 2.10 build
  3. git
  4. sbt

###In order to run this demo you will need to download the source from GitHub.

  * Navigate to the directory where you would like to save the code.
  * Execute the following command:
  
  
       `git clone https://github.com/cgilm/BootCampSparkStreaming.git`
  
###To build the demo

  * Create the Cassandra Keyspaces and Tables using the `CreateTable.cql` script
  * Navigate to the root directory of the project where you downloaded
  * Build the Producer with this command:
  
    `sbt producer/package`
      
  * Build the Consumer with this command:
  
    `sbt consumer/package`
  
###To run the demo

This assumes you already have Kafka and DSE up and running and configured as in the steps above.

  * From the root directory of the project start the producer app
  
    `sbt producer/run`
    
  
  * From the root directory of the project start the consumer app
  
    `dse spark-submit --master spark://127.0.0.1:7077 --packages org.apache.spark:spark-streaming-kafka_2.10:1.4.1 --class com.datastax.demo.fraudprevention.TransactionConsumer consumer/target/scala-2.10/consumer_2.10-0.1.jar`
    
  
