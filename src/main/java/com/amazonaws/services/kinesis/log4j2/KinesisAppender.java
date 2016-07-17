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

import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.AppenderLoggingException;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.util.Booleans;
import org.apache.logging.log4j.status.StatusLogger;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * Log4J Appender implementation to support sending data from java applications
 * directly into a Kinesis stream.
 * Upgraded to log4j2
 * <p>
 * More details are available <a
 * href="https://github.com/awslabs/kinesis-log4j-appender">here</a>
 */
@Plugin(name = "KinesisAppender", category = "Core",
        elementType = "appender", printObject = true)
public class KinesisAppender extends AbstractAppender {

    // The kinesis client
    private AmazonKinesisAsyncClient kinesisClient = null;
    // Async call flag
    private boolean asyncCall;
    // Kinesis stream name
    private String streamName;
    // Failed on error, such as Kinesis erro
    private boolean ignoreExceptions = true;
    // Pattern for the message
    private Layout<? extends Serializable> layout;
    // Buffer size for task buffer
    private int bufferSize = AppenderConstants.DEFAULT_BUFFER_SIZE;

    private static final Logger LOGGER = StatusLogger.getLogger();

    /**
     * List of results sent to Kinesis, can be used for inspection of messages
     *
     * @return the Collection used to store the messages
     */
    public List<PutRecordResult> getResultList() {
        return resultList;
    }

    /**
     * ResultList if available, will be used to store the messages sent to Kinesis
     */
    public void setResultList(List<PutRecordResult> resultList) {
        this.resultList = resultList;
    }

    private List<PutRecordResult> resultList = null;

    private static IKinesisClientBuilder kinesisCLientBuilder = new KinesisClientBuilder();

    /**
     * Returns count of tasks scheduled to send records to Kinesis. Since
     * currently each task maps to sending one record, it is equivalent to number
     * of records in the buffer scheduled to be sent to Kinesis.
     *
     * @return count of tasks scheduled to send records to Kinesis.
     */
    public int getBufferSize() { return bufferSize; }

    /**
     * Instantiate a KinesisAppender and set the output destination
     *
     * @param name             The name of the Appender.
     * @param filter           The Log events filter
     * @param layout           The layout to format the message.
     * @param ignoreExceptions Failed on Error, such as Kinesis stream not found
     * @param kinesisClient    The Kinesis client
     * @param streamName       Kinesis stream name
     * @param asyncCall        Will be fire-and-forget if true
     * @param bufferSize       Size of buffer to be sent to Kinesis
     */
    protected KinesisAppender(String name, Filter filter, Layout<? extends Serializable> layout, boolean ignoreExceptions, AmazonKinesisAsyncClient kinesisClient, String streamName, boolean asyncCall, int bufferSize) {
        super(name, filter, layout, ignoreExceptions);
        this.kinesisClient = kinesisClient;
        this.asyncCall = asyncCall;
        this.streamName = streamName;
        this.layout = layout;
        this.ignoreExceptions = ignoreExceptions;
        this.bufferSize = bufferSize;
    }

    /**
     * Publish the event.
     * @param logEvent The LogEvent.
     */
    @Override
    public void append(final LogEvent logEvent) {

        try {
            if (kinesisClient != null) {
                Layout layout = getLayout();
                final byte[] bytes = layout.toByteArray(logEvent);
                ByteBuffer data = ByteBuffer.wrap(bytes);
                Future<PutRecordResult> result =
                        kinesisClient
                                .putRecordAsync(new PutRecordRequest().withPartitionKey(
                                        UUID.randomUUID().toString())
                                        .withStreamName(streamName).withData(data));
                LOGGER.debug("Post to kinesis: " + logEvent.getMessage());
                if(!asyncCall){
                    PutRecordResult reply = result.get();
                    if(null != getResultList())
                        getResultList().add(reply);
                }
            }
        } catch (final Exception e) {
            LOGGER.error("Unable to write to kinesis for appender [{}].",  this.getName(), e);
            if(!ignoreExceptions){
                throw new AppenderLoggingException(
                        "Unable to write to kinesis in appender: " + e.getMessage(), e);
            }
        } finally {
        }
    }

    /**
     * Appender stop
     */
    @Override
    public void stop() {
        super.stop();
        if (kinesisClient != null) {
            kinesisClient.shutdown();
        }
    }

    /**
     * Factory for the appender
     */
    @PluginFactory
    public static com.amazonaws.services.kinesis.log4j2.KinesisAppender createAppender(
            @PluginAttribute("name") final String inName,
            @PluginAttribute("streamName") final String inStreamName,
            @PluginAttribute("region") final String inRegionInput,
            @PluginAttribute("ignoreExceptions") final String inIgnoreExceptions,
            @PluginAttribute("asyncCall") final String inAsyncCall,
            @PluginAttribute("endpoint") final String inEndpoint,
            @PluginAttribute("enabled") final String inEnabled,
            @PluginAttribute("bufferSize") final String inBufferSize,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginElement("Filters") final Filter filter
    ){
        String name = inName;

        if (inStreamName == null) {
            LOGGER.error("Invalid configuration - streamName cannot be null for appender: " + name);
        }

        if (layout == null) {
            LOGGER.error("Invalid configuration - No layout for appender: " + name);
        }

        int bufferSize = AppenderConstants.DEFAULT_BUFFER_SIZE;
        if (inBufferSize != null){
            bufferSize = Integer.parseInt(inBufferSize);
        }

        boolean ignoreExceptions = Booleans.parseBoolean(inIgnoreExceptions, true);
        boolean async = Booleans.parseBoolean(inAsyncCall, false);
        boolean enabled = Booleans.parseBoolean(inEnabled, false);

        final int threadCount = AppenderConstants.DEFAULT_THREAD_COUNT;

        AmazonKinesisAsyncClient kinesisClient = null;
        if(!enabled){
            return new KinesisAppender(name, filter, layout, ignoreExceptions, null, inStreamName, async, bufferSize);
        }else{
            try{
                kinesisClient = getKinesisCLientBuilder().buildAmazonKinesisAsyncClient(name, inStreamName, inRegionInput, inEndpoint, bufferSize, threadCount);
            }catch(Exception e){
                LOGGER.error("Error creating kinesisClient", e);
                if(!ignoreExceptions){
                    throw(e);
                }
            }finally{
                return new KinesisAppender(name, filter, layout, ignoreExceptions, kinesisClient, inStreamName, async, bufferSize);
            }
        }
    }

    public static IKinesisClientBuilder getKinesisCLientBuilder() {
        return kinesisCLientBuilder;
    }

    public static void setKinesisCLientBuilder(IKinesisClientBuilder kinesisCLientBuilder) {
        KinesisAppender.kinesisCLientBuilder = kinesisCLientBuilder;
    }
}
