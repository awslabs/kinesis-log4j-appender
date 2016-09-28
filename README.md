# Log4J Appender for Amazon Kinesis

The Log4J Appender for Amazon Kinesis enables customers to publish logs from their Java applications into an Amazon Kinesis stream. We also provide a publisher application which uses the Log4J Appender for Amazon Kinesis to publish data from a file into an Amazon Kinesis stream.

## Requirements
* [AWS SDK for Java 1.7](http://aws.amazon.com/sdkforjava)
* [Java 1.7 (Java SE 7)](http://www.oracle.com/technetwork/java/javase/overview/index.html) or later
* [Apache Log4j 1.2.x](http://logging.apache.org/log4j/1.2/)

## Overview

The Log4J Appender for Amazon Kinesis:
* Buffers log messages in memory
* Uses the Amazon Kinesis AsyncClient from the AWS SDK for Java
* Uses a ThreadPoolExecutor backed by a LinkedBlockingQueue. In most cases, this results in the log message simply being added to the in-memory queue. However, if the queue is full, the logging call will block until there is room in the buffer (e.g. some log entries are successfully sent to Amazon Kinesis and removed from the queue).
* Uses an exponential backoff strategy with a configurable number of maxRetries in case of failures.

If a message is not successfully sent to Amazon Kinesis even after the retry attempts, then it is sent to the fallback handler (and no longer sent to Amazon Kinesis).

Note that the current implementation doesn't insert records in the same order as they are logged. One way to get ordering is by including a timestamp in the log entries and then sorting them at the consumer side of the Amazon Kinesis stream.

## Configuration Options

| **Configuration** | **Default** | **Description**
| :----------|:----------:|:----------
| log4j.appender.[APPENDER_NAME].kinesisEndpoint| kinesis.us-east-1.amazonaws.com | Kinesis Endpoint (AKA AWS Region) in which the stream is available
| log4j.appender.[APPENDER_NAME].streamName| | Stream name to which data is to be published
| log4j.appender.[APPENDER_NAME].encoding | UTF-8 | Encoding used to convert log message strings into bytes before sending
| log4j.appender.[APPENDER_NAME].maxRetries | 3 | Maximum number of retries when calling Kinesis APIs to publish a log message.
| log4j.appender.[APPENDER_NAME].threadCount | 20 | Number of parallel threads for publishing logs to configured Kinesis stream
| log4j.appender.[APPENDER_NAME].bufferSize | 2000 | Maximum number of outstanding log messages to keep in memory
| log4j.appender.[APPENDER_NAME].shutdownTimeout | 30 | Seconds to send buffered messages before application JVM quits normally
| log4j.appender.[APPENDER_NAME].endpoint | kinesis.us-east-1.amazonaws.com | Amazon Kinesis endpoint to make requests to, if configured this overrides default endpoint for the configured region
| log4j.appender.[APPENDER_NAME].region | us-east-1 | Amazon Kinesis endpoint in this configured region will be used for making requests unless overridden by log4j.appender.[APPENDER_NAME].endpoint

## Recommendations/Information

We recommend that customers:
* Configure parameters such as buffer size, number of threads, and JVM heap size based on the needs of their use case.
* Configure an additional appender (for example: DailyRollingFileAppender), so that they will retain logs even in the rare cases when the log entries may not be successfully sent to Amazon Kinesis.
* Pick a shutdownTimeout that allows the appender to send all outstanding log entries to Amazon Kinesis before exiting.

## Proxy Configuration

For customers who connect to the Internet through a proxy server, the Log4J
Appender for Amazon Kinesis will pick up the [Java proxy system
properties](https://docs.oracle.com/javase/6/docs/technotes/guides/net/proxies.html)
and update the underlying ClientConfiguration instance.

Supported system properies are http.proxyHost, http.proxyPort, http.proxyUser,
http.proxyPassword, and http.auth.ntlm.domain. For NTLM authentication on Windows, the
COMPUTERNAME environment variable is used to set the proxy workstation.

## Building from source
* Download and install [Apache Maven 3.x](http://maven.apache.org/download.cgi)
* Clone this github repository to a new directory (e.g. by default, this is `kinesis-log4j-appender`) by using following `git` command on shell prompt:

  ```bash
  git clone https://github.com/awslabs/kinesis-log4j-appender
  ```
  This would download the repository to `kinesis-log4j-appender` directory.
* On the shell prompt, run `mvn clean install` in `kinesis-log4j-appender` directory and wait for packages to build. If the build is successful, you should see a message like the following:

  ```properties
  ------
  [INFO]
  ------
  [INFO] ------------------------------------------------------------------------
  [INFO] BUILD SUCCESS
  [INFO] ------------------------------------------------------------------------
  [INFO] Total time: 5.091s
  [INFO] Finished at: Wed Feb 12 17:35:03 GMT 2014
  [INFO] Final Memory: 52M/185M
  [INFO] ------------------------------------------------------------------------
  ```
  This command creates a `target` directory inside `kinesis-log4j-appender` directory.

## Publishing a File Using This Library
* After building this repository, copy **kinesis-log4j-appender-1.0.0.jar** from `target` directory to some other directory (such as **KinesisLoggingTest**).
* Create an Amazon Kinesis stream (such as **testStream**).
* Copy sample log4j configuration file from [here](src/main/resources/log4j-sample.properties) and save it as **log4j.properties** in **KinesisLoggingTest** directory.
* Create the credentials file, ***AwsCredentials.properties*** in **KinesisLoggingTest** directory. It must contain following keys:

  ```properties
  accessKey=<AWS_ACCESS_KEY>
  secretKey=<AWS_SECRET_KEY>
  ```
* Run the following command on command prompt from **KinesisLoggingTest** directory:

  On Mac OS X/Linux/Unix:
  ```bash
  ${JAVA_HOME}/bin/java -cp .:./kinesis-log4j-appender-1.0.0.jar com.amazonaws.services.kinesis.log4j.FilePublisher <path_to_sample_log_file>
  ```
  On Windows:
  ```bat
  %JAVA_HOME%/bin/java -cp .;./kinesis-log4j-appender-1.0.0.jar com.amazonaws.services.kinesis.log4j.FilePublisher <path_to_sample_log_file>
  ```
  If Java is already in the environment configuration, you can use the following command directly (without reference to JAVA_HOME):
  ```bash
  java -cp ...
  ```
  The application begins publishing records to the configured Amazon Kinesis stream, with the following output:
  ```properties
  INFO [main] (FilePublisher.java:64) - Started reading: access_log_1
  [continues...]
  DEBUG [main] (FilePublisher.java:79) - Total 6100 records written to logger
  DEBUG [main] (FilePublisher.java:79) - Total 6200 records written to logger
  DEBUG [main] (FilePublisher.java:79) - Total 6300 records written to logger
  DEBUG [main] (FilePublisher.java:79) - Total 6400 records written to logger
  DEBUG [main] (FilePublisher.java:79) - Total 6500 records written to logger
  DEBUG [main] (FilePublisher.java:79) - Total 6600 records written to logger
  [continues...]
  DEBUG [pool-1-thread-12] (AsyncPutCallStatsReporter.java:62) - Appender (KINESIS) made 12000 successful put requests out of total 12000 in 1 minute, 14 seconds and 250 milliseconds since start
  [continues...]
  ```

## Related Resources
* [Amazon Kinesis Developer Guide](http://docs.aws.amazon.com/kinesis/latest/dev/introduction.html)  
* [Amazon Kinesis API Reference](http://docs.aws.amazon.com/kinesis/latest/APIReference/Welcome.html)
* [AWS SDK for Java](http://aws.amazon.com/sdkforjava)
* [Apache Log4j 1.2.x](http://logging.apache.org/log4j/1.2/)
* [Building a Project with Maven](http://maven.apache.org/run-maven/index.html)
