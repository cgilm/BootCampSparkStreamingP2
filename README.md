#Bootcamp Spark Streaming Project #2

In order to compile and run this project, It is assumed that you have the following installed and available on your local system.

  1. Datastax Enterprise 5.0.x
  2. Apache Kafka 0.9.0.1, I used the Scala 2.10 build
  3. git
  4. sbt
  5. [Ubuntu Install Instructions!](docs/prerequisites.md)

###In order to run this download you will need to download the source from this GitHub.

  * Navigate to the directory where you would like to save the code.
  * Execute the following command:
  
  
       `git clone https://github.com/cgilm/BootCampSparkStreamingP1.git`

### Project Guidelines
  1. Open `consumer/src/main/scala/TransactionConsumer.scala`
  2. Find the TODO section and complete the code
  3. The goal is to use the provided KAFKA direct stream window to calculate
      - Use the 1min window to calculate 
        - Count of Transactions
        - Count of Approved Transactions
        - Percentage of Approved Transactions
      - Pull data from txn_count_min table to add the following
        - Running count of transactions within the hour
        - Running count of approved transactions within the hour
        - Running percentage of approved transactions within the hour
  6. Save the results to the txn_count_min table in Cassandra

###To build the project

  * Create the Cassandra Keyspaces and Tables using the `CreateTables.cql` script
    `cqlsh -f resources/CreateTables.cql`    

  * Build the Producer from the project root with this command:
  
    `sbt producer/package`
      
  * Build the Consumer from the project root  with this command:
  
    `sbt consumer/package`
  
###To run the project
  * From the root directory of the project start the producer app
  
    `sbt producer/run`
    
  
  * From the root directory of the project start the consumer app
  
    `dse spark-submit --master spark://<YOUR SPARK MASTER HERE>:7077 --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.1 --class com.datastax.demo.fraudprevention.TransactionConsumer consumer/target/scala-2.10/consumer_2.10-0.1.jar`
    
  
