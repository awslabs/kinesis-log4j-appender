package com.amazonaws.services.kinesis.log4j.helpers;

import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import mockit.Mock;
import mockit.MockUp;
import org.apache.log4j.Logger;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests the {@link com.amazonaws.services.kinesis.log4j.helpers.AsyncPutCallStatsReporter} class
 */
public class AsyncPutCallStatsReporterTest {
    /**
     * Verifies AsyncPutCallStatsReporter.onError() logs the exception as expected.
     */
    @Test
    public void testLogsExceptionOnError() {
        AsyncPutCallStatsReporter reporter = new AsyncPutCallStatsReporter("test-stream-name");

        new MockUp<Logger>() {
            @SuppressWarnings("unused")
            @Mock
            public void error(Object message, Throwable t) {
                assertThat(t, instanceOf(Exception.class));
                assertThat(t.getMessage(), is(equalTo("Expected Exception.")));
                assertThat((String) message, is(equalTo("Failed to publish a log entry to kinesis using appender: test-stream-name")));
            }
        };

        reporter.onError(new Exception("Expected Exception."));
    }

    /**
     * Verifies AsyncPutCallStatsReporter.onSuccess() logs the debug message as expected.
     */
    @Test
    public void testLogsDebugMessageWhenDebugEnabled() {
        new MockUp<Logger>() {
            @SuppressWarnings("unused")
            @Mock
            public boolean isDebugEnabled() { return true; }
            @SuppressWarnings("unused")
            @Mock
            public void debug(Object message) {
                assertThat((String) message, startsWith("Appender (test-stream-name) made 3000 successful put requests out of total 3000"));
            }
        };

        AsyncPutCallStatsReporter reporter = new AsyncPutCallStatsReporter("test-stream-name");
        PutRecordRequest request = new PutRecordRequest();
        PutRecordResult result = new PutRecordResult();

        // we need 3000 requests before we'll actually hit the debug
        for (int i = 0; i < 3000; i++) {
            reporter.onSuccess(request, result);
        }
    }

    /**
     * Verifies AsyncPutCallStatsReporter.onSuccess() does not log debug message when debug is not enabled.
     */
    @Test
    public void testDoesNotLogDebugMessageWhenDebugNotEnabled() {
        new MockUp<Logger>() {
            @SuppressWarnings("unused")
            @Mock
            public boolean isDebugEnabled() { return false; }
            @SuppressWarnings("unused")
            @Mock
            public void debug(Object message) {
                fail("Should not log debug message...");
            }
        };

        AsyncPutCallStatsReporter reporter = new AsyncPutCallStatsReporter("test-stream-name");
        PutRecordRequest request = new PutRecordRequest();
        PutRecordResult result = new PutRecordResult();

        // we need 3000 requests before we'll actually hit the debug
        for (int i = 0; i < 3000; i++) {
            reporter.onSuccess(request, result);
        }
    }
}
