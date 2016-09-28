package com.amazonaws.services.kinesis.log4j;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.log4j.helpers.AsyncPutCallStatsReporter;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.services.kinesis.model.StreamStatus;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static mockit.Deencapsulation.invoke;
import static mockit.Deencapsulation.setField;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * Tests the {@link com.amazonaws.services.kinesis.log4j.KinesisAppender} class
 */
public class KinesisAppenderTest {

    private final Layout layout = new PatternLayout("%5p [%t] (%F:%L) - %m%n");

    private KinesisAppender appender;

    @Mocked ThreadPoolExecutor threadPoolExecutor;
    @Mocked AmazonKinesisAsyncClient kinesisClient;
    @Mocked StreamDescription streamDescription;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    /**
     * Configures log4J and sets the log level for the test suite.
     */
    @BeforeClass
    public static void setUp() {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.ERROR);
    }

    /**
     * Sets up the {@link com.amazonaws.services.kinesis.log4j.KinesisAppender} for each of the tests
     */
    @Before
    public void setUpTest() {
        appender = new KinesisAppender();
        appender.setName("unit-test-kinesis-appender");
        appender.setStreamName("unit-test-kinesis-stream");
        appender.setLayout(layout);

        assertThat(appender.getName(), is("unit-test-kinesis-appender"));
        assertThat(appender.getStreamName(), is("unit-test-kinesis-stream"));
        assertThat(appender.getLayout(), is(layout));
    }

    /**
     * Verifies the appender throws an exception when the Kinesis stream has not been defined.
     */
    @Test
    public void testActivateOptionsThrowsExceptionWithNullStreamName() {
        KinesisAppender appender = new KinesisAppender();

        assertThat(appender.getStreamName(), is(nullValue()));

        exception.expect(IllegalStateException.class);
        exception.expectMessage("Invalid configuration - streamName cannot be null for appender: null");
        appender.activateOptions();
    }

    /**
     * Verifies the appender throws an exception when a {@link org.apache.log4j.Layout} has not been defined.
     */
    @Test
    public void testActivateOptionsThrowsExceptionWithNullLayout() {
        appender.setLayout(null);

        assertThat(appender.getLayout(), is(nullValue()));

        exception.expect(IllegalStateException.class);
        exception.expectMessage("Invalid configuration - No layout for appender: unit-test-kinesis-appender");
        appender.activateOptions();
    }

    /**
     * Verifies the appender throws an exception when the Kinesis stream has been defined but does not actually exist.
     */
    @Test
    public void testActivateOptionsFailsWhenKinesisStreamDoesNotExist() {
        new Expectations() {
            {
                kinesisClient.describeStream(anyString);
                result = new ResourceNotFoundException("Expected Exception");
            }
        };

        exception.expect(IllegalStateException.class);
        exception.expectMessage("Stream unit-test-kinesis-stream doesn't exist for appender: unit-test-kinesis-appender");
        appender.activateOptions();
    }

    /**
     * Verifies the appender throws an exception if the Kinesis stream is being created.
     */
    @Test
    public void testActivateOptionsFailsWhenKinesisStreamIsBeingCreated() {
        new Expectations() {
            {
                streamDescription.getStreamStatus();
                returns(StreamStatus.CREATING.name());
            }
        };

        exception.expect(IllegalStateException.class);
        exception.expectMessage("Stream unit-test-kinesis-stream is not ready (in active/updating status) for appender: unit-test-kinesis-appender");
        appender.activateOptions();
    }

    /**
     * Verifies the appender throws an exception if the Kinesis stream is being deleted.
     */
    @Test
    public void testActivateOptionsFailsWhenKinesisStreamIsBeingDeleted() {
        new Expectations() {
            {
                streamDescription.getStreamStatus();
                returns(StreamStatus.DELETING.name());
            }
        };

        exception.expect(IllegalStateException.class);
        exception.expectMessage("Stream unit-test-kinesis-stream is not ready (in active/updating status) for appender: unit-test-kinesis-appender");
        appender.activateOptions();
    }

    /**
     * Verifies the appender is configured with the default region when the endpoint is an empty string.
     */
    @Test
    public void testSetEndpointWithEmptyStringAppendsToDefaultRegion() {
        new Expectations() {
            {
                streamDescription.getStreamStatus();
                returns(StreamStatus.ACTIVE.name());
            }
        };

        appender.setEndpoint(String.valueOf(""));
        appender.activateOptions();

        assertThat(appender.getEndpoint(), is(String.valueOf("")));
        assertThat(appender.getRegion(), is(AppenderConstants.DEFAULT_REGION));
    }

    /**
     * Verifies the appender properly handles setting the Kinesis endpoint.
     */
    @Test
    public void testSetEndpoint() {
        new Expectations() {
            {
                streamDescription.getStreamStatus();
                returns(StreamStatus.ACTIVE.name());
            }
        };

        appender.setEndpoint("kinesis.us-east-1.amazonaws.com");
        appender.activateOptions();

        assertThat(appender.getEndpoint(), is("kinesis.us-east-1.amazonaws.com"));

        new Verifications() {
            {
                kinesisClient.setEndpoint("kinesis.us-east-1.amazonaws.com",
                        AppenderConstants.DEFAULT_SERVICE_NAME,
                        AppenderConstants.DEFAULT_REGION);
                times = 1;

            }
        };
    }

    /**
     * Verifies the appender properly handles the situation where both endpoint and region have been defined.
     */
    @Test
    public void testAppenderProperlyHandlesSettingRegionAndEndpoint() {
        new Expectations() {
            {
                streamDescription.getStreamStatus();
                returns(StreamStatus.ACTIVE.name());
            }
        };

        new MockUp<Logger>() {
            @SuppressWarnings("unused")
            @Mock(minInvocations = 1)
            public void warn(Object message) {
                assertThat((String) message, startsWith("Received configuration for both region as well as Amazon Kinesis endpoint"));
            }
        };

        appender.setRegion(AppenderConstants.DEFAULT_REGION);
        appender.setEndpoint("kinesis.us-east-1.amazonaws.com");
        appender.activateOptions();

        assertThat(appender.getEndpoint(), is("kinesis.us-east-1.amazonaws.com"));
        assertThat(appender.getRegion(), is(AppenderConstants.DEFAULT_REGION));

        new Verifications() {
            {
                kinesisClient.setEndpoint("kinesis.us-east-1.amazonaws.com",
                        AppenderConstants.DEFAULT_SERVICE_NAME,
                        AppenderConstants.DEFAULT_REGION);
                times = 1;

            }
        };
    }

    /**
     * Verifies the appender throws an exception when the Kinesis stream is set to an empty string.
     */
    @Test
    public void testSetStreamNameThrowsExceptionWithEmptyString() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("streamName cannot be blank");
        appender.setStreamName(String.valueOf(""));
    }

    /**
     * Verifies the appender sets the Kinesis stream properly when defined.
     */
    @Test
    public void testSetStreamNameWithValidString() {
        final String name = "local-test-stream-name";
        appender.setStreamName(name);
        assertThat(appender.getStreamName(), is(name));
    }

    /**
     * Verifies the appender throws an exception when encoding is not defined.
     */
    @Test
    public void testSetEncodingThrowsExceptionWithEmptyString() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("encoding cannot be blank");
        appender.setEncoding(String.valueOf(""));
    }

    /**
     * Verifies the appender sets encoding when properly defined.
     */
    @Test
    public void testSetEncodingWithValidEncoding() {
        appender.setEncoding(AppenderConstants.DEFAULT_ENCODING);
        assertThat(appender.getEncoding(), is(AppenderConstants.DEFAULT_ENCODING));
    }

    /**
     * Verifies the appender throws an exception when MaxRetries is set to 0.
     */
    @Test
    public void testSetMaxRetriesThrowsExceptionWithZero() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("maxRetries must be > 0");
        appender.setMaxRetries(0);
    }

    /**
     * Verifies the appender sets maxRetries when properly defined.
     */
    @Test
    public void testSetMaxRetriesWithValidRetries() {
        appender.setMaxRetries(AppenderConstants.DEFAULT_MAX_RETRY_COUNT);
        assertThat(appender.getMaxRetries(), is(AppenderConstants.DEFAULT_MAX_RETRY_COUNT));
    }

    /**
     * Verifies the appender throws an exception when bufferSize is set to 0.
     */
    @Test
    public void testSetBufferSizeThrowsExceptionWithZero() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("bufferSize must be > 0");
        appender.setBufferSize(0);
    }

    /**
     * Verifies the appender sets bufferSize when properly defined.
     */
    @Test
    public void testSetBufferSizeWithValidSize() {
        appender.setBufferSize(AppenderConstants.DEFAULT_BUFFER_SIZE);
        assertThat(appender.getBufferSize(), is(AppenderConstants.DEFAULT_BUFFER_SIZE));
    }

    /**
     * Verifies the appender throws an exception when threadCount is set to 0.
     */
    @Test
    public void testSetThreadCountThrowsExceptionWithZero() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("threadCount must be > 0");
        appender.setThreadCount(0);
    }

    /**
     * Verifies the appender sets threadCount when properly defined.
     */
    @Test
    public void testSetThreadCountWithValidCount() {
        final int threadCount = 2;
        appender.setThreadCount(threadCount);
        assertThat(appender.getThreadCount(), is(threadCount));
    }

    /**
     * Verifies the appender throws an exception when shutdownTimeout is set to 0.
     */
    @Test
    public void testSetShutdownTimeoutThrowsExceptionWithZero() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("shutdownTimeout must be > 0");
        appender.setShutdownTimeout(0);
    }

    /**
     * Verifies the appender sets threadCount when properly defined.
     */
    @Test
    public void testSetShutdownTimeoutWithValidTimeout() {
        appender.setShutdownTimeout(AppenderConstants.DEFAULT_SHUTDOWN_TIMEOUT_SEC);
        assertThat(appender.getShutdownTimeout(), is(AppenderConstants.DEFAULT_SHUTDOWN_TIMEOUT_SEC));
    }

    /**
     * Verifies the appender sets the AWS region when properly defined.
     */
    @Test
    public void testSetRegionWithValidRegion() {
        appender.setRegion(Regions.EU_WEST_1.getName());
        assertThat(appender.getRegion(), is(Regions.EU_WEST_1.getName()));
    }

    /**
     * Verifies the appender requires a {@link org.apache.log4j.Layout} to be defined.
     */
    @Test
    public void testAppenderRequiresLayout() {
        assertThat(appender.requiresLayout(), is(true));
    }

    /**
     * Verifies the appender properly handles a graceful shutdown.
     *
     * @throws InterruptedException
     */
    @Test
    public void testAppenderCanCloseWithGracefulShutdown() throws InterruptedException {
        new Expectations() {
            {
                streamDescription.getStreamStatus();
                returns(StreamStatus.ACTIVE.name());

                // simulate a graceful shutdown
                threadPoolExecutor.awaitTermination(anyInt, TimeUnit.SECONDS);
                returns(true);
            }
        };

        new MockUp<Logger>() {
            @SuppressWarnings("unused")
            @Mock(maxInvocations = 0)
            public void error(Object message) { }
        };

        appender.activateOptions();
        appender.close();

        new Verifications() {
            {
                threadPoolExecutor.shutdown(); times = 1;
                kinesisClient.shutdown(); times = 1;
            }
        };
    }

    /**
     * Verifies the appender logs an error when an ungraceful shutdown occurs with an empty task queue.
     *
     * @param taskQueue a {@link mockit.Mocked}, empty {@link java.util.concurrent.BlockingQueue<Runnable>} task queue
     * @throws InterruptedException
     */
    @Test
    public void testAppenderLogsErrorWithUngracefulShutdownAndEmptyTaskQueue(@Mocked final BlockingQueue<Runnable> taskQueue) throws InterruptedException {
        new Expectations() {
            {
                streamDescription.getStreamStatus();
                returns(StreamStatus.ACTIVE.name());

                threadPoolExecutor.awaitTermination(anyInt, TimeUnit.SECONDS);
                returns(false);

                taskQueue.size();
                returns(0);
            }
        };

        new MockUp<Logger>() {
            @SuppressWarnings("unused")
            @Mock(invocations = 1)
            public void error(Object message) {
                assertThat((String) message, startsWith("Kinesis Log4J Appender"));
            }
        };

        appender.activateOptions();
        appender.close();

        new Verifications() {
            {
                threadPoolExecutor.shutdown(); times = 1;
                kinesisClient.shutdown(); times = 1;
            }
        };
    }

    /**
     * Verifies the appender logs an error when an ungraceful shutdown occurs with messages in the task queue.
     *
     * @param taskQueue a {@link mockit.Mocked} {@link java.util.concurrent.BlockingQueue<Runnable>} task queue
     * @throws InterruptedException
     */
    @Test
    public void testAppenderLogsErrorWithUngracefulShutdownAndNonEmptyTaskQueue(@Mocked final BlockingQueue<Runnable> taskQueue) throws InterruptedException {
        new Expectations() {
            {
                streamDescription.getStreamStatus();
                returns(StreamStatus.ACTIVE.name());

                threadPoolExecutor.awaitTermination(anyInt, TimeUnit.SECONDS);
                returns(false);

                taskQueue.size();
                returns(0,10,10);
            }
        };

        new MockUp<Logger>() {
            @SuppressWarnings("unused")
            @Mock(invocations = 1)
            public void error(Object message) {
                assertThat((String) message, startsWith("Kinesis Log4J Appender"));
            }
        };

        appender.activateOptions();
        appender.close();

        new Verifications() {
            {
                threadPoolExecutor.shutdown(); times = 1;
                kinesisClient.shutdown(); times = 1;
            }
        };
    }

    /**
     * Verifies the appender sends a {@link org.apache.log4j.spi.LoggingEvent} to the Kinesis client
     *
     * @param event a {@link mockit.Mocked} {org.apache.log4j.spi.LoggingEvent}
     */
    @Test
    public void testAppenderLogsEvent(@Mocked final LoggingEvent event) {
        new Expectations() {
            {
                streamDescription.getStreamStatus();
                returns(StreamStatus.ACTIVE.name());
            }
        };

        appender.activateOptions();
        appender.append(event);

        new Verifications() {
            {
                kinesisClient.putRecordAsync((PutRecordRequest) any, withInstanceOf(AsyncPutCallStatsReporter.class));
                times = 1;
            }
        };
    }

    /**
     * Verifies the appender logs an error when there is an attempt to log an event after initialization has failed.
     *
     * @param event a {@link mockit.Mocked} {org.apache.log4j.spi.LoggingEvent}
     */
    @Test
    public void testAppenderLogsErrorWhenInitializationHasFailed(@Mocked final LoggingEvent event) {
        new Expectations() {
            {
                setField(appender, "initializationFailed", true);
            }
        };

        exception.expect(IllegalStateException.class);
        exception.expectMessage(startsWith("Check the configuration"));
        appender.append(event);
    }

    /**
     * Verifies the appender handles a buffer overflow when attempting to log a message to Kinesis
     *
     * @param event a {@link mockit.Mocked} {org.apache.log4j.spi.LoggingEvent}
     */
    @Test
    public void testAppenderHandlesByteBufferException(@Mocked final LoggingEvent event) {
        new Expectations(ByteBuffer.class) {
            {
                ByteBuffer.wrap((byte[]) any);
                result = new IndexOutOfBoundsException("Expected Exception");
            }
        };

        // at this point, this is the only way to determing graceful shutdown
        new MockUp<Logger>() {
            @SuppressWarnings("unused")
            @Mock(invocations = 1)
            public void error(Object message) {
                assertThat((String) message, startsWith("Failed to schedule log entry"));
            }
        };

        appender.append(event);
    }

    /**
     * Verifies the appender handles an exception from the Kinesis Web Service when attempting to log a message.
     *
     * @param event a {@link mockit.Mocked} {org.apache.log4j.spi.LoggingEvent}
     */
    @Test
    public void testAppenderHandlesAmazonServiceException(@Mocked final LoggingEvent event) {
        new Expectations() {
            {
                streamDescription.getStreamStatus();
                returns(StreamStatus.ACTIVE.name());

                kinesisClient.putRecordAsync((PutRecordRequest) any, withInstanceOf(AsyncPutCallStatsReporter.class));
                result = new AmazonServiceException("Expected Exception");
            }
        };

        new MockUp<Logger>() {
            @SuppressWarnings("unused")
            @Mock(invocations = 1)
            public void error(Object message) {
                assertThat((String) message, startsWith("Failed to schedule log entry"));
            }
        };

        appender.activateOptions();
        appender.append(event);
    }

    /**
     * Verifies the appender handles an exception from the Kinesis Client when attempting to log a message.
     *
     * @param event a {@link mockit.Mocked} {org.apache.log4j.spi.LoggingEvent}
     */
    @Test
    public void testAppenderHandlesAmazonClientException(@Mocked final LoggingEvent event) {
        new Expectations() {
            {
                streamDescription.getStreamStatus();
                returns(StreamStatus.ACTIVE.name());

                kinesisClient.putRecordAsync((PutRecordRequest) any, withInstanceOf(AsyncPutCallStatsReporter.class));
                result = new AmazonClientException("Expected Exception");
            }
        };

        new MockUp<Logger>() {
            @SuppressWarnings("unused")
            @Mock(invocations = 1)
            public void error(Object message) {
                assertThat((String) message, startsWith("Failed to schedule log entry"));
            }
        };

        appender.activateOptions();
        appender.append(event);
    }
}
