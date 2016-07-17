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
package test.com.amazonaws.services.kinesis.log4j2;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.log4j2.IKinesisClientBuilder;
import com.amazonaws.services.kinesis.log4j2.KinesisAppender;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.status.StatusConsoleListener;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/**
 * KinesisAppender Tester to assert the normal publishing, and exceptions handling flow
 */
@RunWith(MockitoJUnitRunner.class)
public class KinesisAppenderTest {

    private Logger logger;
    @Before
    public void setup(){
        logger = (Logger)LogManager.getLogger(KinesisAppender.class);
    }

    @After
    public void teardown(){
        for(Appender appender:logger.getAppenders().values()){
            logger.removeAppender(appender);
        }
    }

    /*
    Mock good kinesis client
     */
    private AmazonKinesisAsyncClient mockKinesisClient(){
        client = mock(AmazonKinesisAsyncClient.class);
        when(client.putRecordAsync(any(PutRecordRequest.class))).thenReturn(null);
        return(client);
    }

    /*
    Mock bad kinesis client that throws exception when putRecordAsync
     */
    private AmazonKinesisAsyncClient mockKinesisClientWithConnectionFail(){
        client = mock(AmazonKinesisAsyncClient.class);
        when(client.putRecordAsync(any(PutRecordRequest.class))).thenThrow(new AmazonServiceException("Kinesis exception"));
        return(client);
    }

    /*
    Mock the Kinesis builder with Kinesis client for various scenarios
     */
    private IKinesisClientBuilder getMockBuilder(AmazonKinesisAsyncClient kinesisClient){
        Assert.assertNotNull(kinesisClient);
        IKinesisClientBuilder kinesisBuilderMock = mock(IKinesisClientBuilder.class);
        when(kinesisBuilderMock.buildAmazonKinesisAsyncClient(
                any(String.class),
                any(String.class),
                any(String.class),
                any(String.class),
                any(Integer.class),
                any(Integer.class))).thenReturn(kinesisClient);

        return(kinesisBuilderMock);
    }

    @Mock
    Appender mockDefaultAppender; // The default appender to test the non-blocking logging when Kinesis appender failed

    @Mock
    StatusConsoleListener mockStatusListener; // Mocked status listener

    @Mock
    AmazonKinesisAsyncClient client; // The mocked Kinesis client

    @Mock
    Layout mockLayout; // The mocked Layout

    /*
    To test the happy flow of message appended
     */
    @Test
    public void test_append() {
        KinesisAppender.setKinesisCLientBuilder(getMockBuilder(mockKinesisClient()));

        mockLayout = mock(Layout.class);
        when(mockLayout.toByteArray(any(LogEvent.class))).thenReturn("".getBytes());

        Logger logger = (Logger)LogManager.getLogger();
        Appender kinesisAppender = KinesisAppender.createAppender("name", "streamName", "regionInput", "false","true", "endpoint","true",null, mockLayout,null);
        kinesisAppender.start();
        Assert.assertNotNull(kinesisAppender);
        logger.addAppender(kinesisAppender);

        logger.error("What an error");
        verifyMessageWithLayout("What an error");
    }

    @Captor
    private ArgumentCaptor<LogEvent> layoutOutput;
    private void verifyMessageWithLayout(String ... messages){
        verify(mockLayout, times(messages.length)).toByteArray(layoutOutput.capture());

        int i=0;
        for(LogEvent event:layoutOutput.getAllValues()) {
            assertEquals(messages[i++], event.getMessage().getFormattedMessage());
        }
    }

    /*
    Message should still be logged when kinesis failed if continueOnError is true
     */
    @Test
    public void test_continueOnError(){
        boolean continueOnError = true;
        KinesisAppender.setKinesisCLientBuilder(getMockBuilder(mockKinesisClientWithConnectionFail()));

        mockLayout = mock(Layout.class);
        when(mockLayout.toByteArray(any(LogEvent.class))).thenReturn("".getBytes());

        Appender kinesisAppender = KinesisAppender.createAppender("name", "streamName", "regionInput","true",String.valueOf(continueOnError), "endpoint","true",null, mockLayout,null);
        kinesisAppender.start();
        Assert.assertNotNull(kinesisAppender);
        logger.addAppender(kinesisAppender);

        // we need the default logger
        mockDefaultAppender = mock(Appender.class);
        when(mockDefaultAppender.getName()).thenReturn("mockDefaultAppender");
        when(mockDefaultAppender.isStarted()).thenReturn(true);
        when(mockDefaultAppender.isStopped()).thenReturn(false);
        logger.addAppender(mockDefaultAppender);

        logger.error("What an error");

        // we should capture the exception itself because we are using KinesisAppender logger
        verifyMockDefaultAppenderErrorMessages("What an error");
    }


    /*
Message should still be logged when appender is disabled
 */
    @Test
    public void test_disabled(){
        boolean continueOnError = true;
        KinesisAppender.setKinesisCLientBuilder(getMockBuilder(mockKinesisClientWithConnectionFail()));

        mockLayout = mock(Layout.class);
        when(mockLayout.toByteArray(any(LogEvent.class))).thenReturn("".getBytes());

        Appender kinesisAppender = KinesisAppender.createAppender("name", "streamName", "regionInput", "true",String.valueOf(continueOnError), "endpoint", "false", null, mockLayout,null);
        kinesisAppender.start();
        Assert.assertNotNull(kinesisAppender);
        logger.addAppender(kinesisAppender);

        // we need the default logger
        mockDefaultAppender = mock(Appender.class);
        when(mockDefaultAppender.getName()).thenReturn("mockDefaultAppender");
        when(mockDefaultAppender.isStarted()).thenReturn(true);
        when(mockDefaultAppender.isStopped()).thenReturn(false);
        logger.addAppender(mockDefaultAppender);

        logger.error("What an error");

        // we should capture the exception itself because we are using KinesisAppender logger
        verifyMockDefaultAppenderErrorMessages("What an error");
    }

    @Captor
    private ArgumentCaptor<LogEvent> captorLoggingEvent;

    private void verifyMockDefaultAppenderErrorMessages(String ... messages) {
        verify(mockDefaultAppender, times(messages.length)).append(captorLoggingEvent.capture());

        int i=0;
        for(LogEvent loggingEvent:captorLoggingEvent.getAllValues()) {
            assertEquals(messages[i++], loggingEvent.getMessage().getFormattedMessage());
        }
    }
}
