/*******************************************************************************
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/apache2.0
 * 
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 ******************************************************************************/
package com.amazonaws.services.kinesis.log4j2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.Appender;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormat;

/**
 * {@code FilePublisher} can be used to push log files to Kinesis stream
 * directly. It reads input file line by line and uses log4j2 to write into a
 * logger instance by name <b>KinesisLogger</b>. Implementation assumes that
 * <b>KinesisLogger</b> is configured to log into a Kinesis stream.
 * 
 * Sample configuration is available in
 * src/main/resources/log4j2-sample.properties
 */
public class FilePublisher {
  private static final Logger LOGGER = (org.apache.logging.log4j.core.Logger) LogManager.getLogger(FilePublisher.class);
  private static final long SLEEP_INTERVAL = 5000;

  private static long getBufferedRecordsCountFromKinesisAppenders() {
    long bufferedRecordsCount = 0;
    for(Appender appender:LOGGER.getAppenders().values()){
      if (appender instanceof KinesisAppender) {
        bufferedRecordsCount += ((KinesisAppender) appender).getBufferSize();
      }
    }
    return bufferedRecordsCount;
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: java " + FilePublisher.class.getName() + " <file_path>");
      System.err.println();
      System.err.println("<file_path>\t-\tabsolute path for the input file, this file will be read line by line and ");
      System.err.println("\t\t\tpublished to Kinesis");
      System.exit(1);
    }
    String fileAbsolutePath = args[0];
    File logFile = new File(fileAbsolutePath);
    if (!logFile.exists() || !logFile.canRead()) {
      System.err.println("File " + args[0] + " doesn't exist or is not readable.");
      System.exit(2);
    }

    Logger kinesisLogger = (org.apache.logging.log4j.core.Logger) LogManager.getLogger("KinesisLogger");
    int i = 0;
    DateTime startTime = DateTime.now();
    BufferedReader reader = new BufferedReader(new FileReader(logFile));
    LOGGER.info("Started reading: " + fileAbsolutePath);
    String line = null;
    while ((line = reader.readLine()) != null) {
      kinesisLogger.info(line);
      i++;
      if (i % 100 == 0 && LOGGER.isDebugEnabled()) {
        LOGGER.debug("Total " + i + " records written to logger");
      }
    }
    reader.close();
    long bufferedRecordsCount = getBufferedRecordsCountFromKinesisAppenders();
    while (bufferedRecordsCount > 0) {
      LOGGER.info("Publisher threads within log4j2 appender are still working on sending " + bufferedRecordsCount
          + " buffered records to Kinesis");
      try {
        Thread.sleep(SLEEP_INTERVAL);
      } catch (InterruptedException e) {
        // do nothing
      }
      bufferedRecordsCount = getBufferedRecordsCountFromKinesisAppenders();
    }
    LOGGER.info("Published " + i + " records from " + fileAbsolutePath + " to the logger, took "
        + PeriodFormat.getDefault().print(new Period(startTime, DateTime.now())));
  }
}
