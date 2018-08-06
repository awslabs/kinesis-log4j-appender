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

import com.amazonaws.services.kinesis.log4j2.helpers.BlockFastProducerPolicy;
import com.amazonaws.services.kinesis.log4j2.helpers.CustomCredentialsProviderChain;
import com.amazonaws.services.kinesis.log4j2.helpers.Validator;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamStatus;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.status.StatusLogger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Kinesis Client builder, externalized to allow injection of mocks during test
 */
public class KinesisClientBuilder implements IKinesisClientBuilder {

    private static final Logger LOGGER = StatusLogger.getLogger();

    public AmazonKinesisAsyncClient buildAmazonKinesisAsyncClient(
            String name,
            String streamName,
            String regionInput,
            String endpoint,
            int bufferSize,
            int threadCount)
    {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setMaxErrorRetry(AppenderConstants.DEFAULT_MAX_RETRY_COUNT);
        clientConfiguration.setRetryPolicy(new RetryPolicy(PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
                PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY,
                AppenderConstants.DEFAULT_MAX_RETRY_COUNT, true));
        clientConfiguration.setUserAgent(AppenderConstants.USER_AGENT_STRING);

        String region = regionInput;
        boolean regionProvided = !Validator.isBlank(region);
        if (!regionProvided) {
            region = AppenderConstants.DEFAULT_REGION;
        }

	    AmazonKinesisAsyncClient kinesisClient = null;
        try {
            BlockingQueue<Runnable> taskBuffer = new LinkedBlockingDeque<Runnable>(bufferSize);
            ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(threadCount, threadCount,
                    AppenderConstants.DEFAULT_THREAD_KEEP_ALIVE_SEC, TimeUnit.SECONDS, taskBuffer, new BlockFastProducerPolicy());
            threadPoolExecutor.prestartAllCoreThreads();
	        kinesisClient = new AmazonKinesisAsyncClient(new CustomCredentialsProviderChain(), clientConfiguration);
            if (!Validator.isBlank(endpoint)) {
                if (regionProvided) {
                    LOGGER
                        .warn("Received configuration for both region as well as Amazon Kinesis endpoint. ("
                                + endpoint
                                + ") will be used as endpoint instead of default endpoint for region ("
                                + region + ")");
                }
                kinesisClient.setEndpoint(endpoint,
                    AppenderConstants.DEFAULT_SERVICE_NAME, region);
            } else {
                kinesisClient.setRegion(Region.getRegion(Regions.fromName(region)));
            }

            DescribeStreamResult describeResult = kinesisClient.describeStream(streamName);
            String streamStatus = describeResult.getStreamDescription().getStreamStatus();
            if (!StreamStatus.ACTIVE.name().equals(streamStatus) && !StreamStatus.UPDATING.name().equals(streamStatus)) {
                LOGGER.error("Stream " + streamName + " is not ready (in active/updating status) for appender: " + name);
            }
        } catch (ResourceNotFoundException rnfe) {
            LOGGER.error("Stream " + streamName + " doesn't exist for appender: " + name, rnfe);
        } catch(Exception e){
            LOGGER.error("Stream " + streamName + " error", e);
        }finally{
	    LOGGER.debug("Created Kinesis appender: " + name);
	    return kinesisClient;
	}
    }
}
