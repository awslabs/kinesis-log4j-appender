package com.amazonaws.services.kinesis.log4j.helpers;

import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Tests the {@link com.amazonaws.services.kinesis.log4j.helpers.BlockFastProducerPolicy} class
 */
public class BlockFastProducerPolicyTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    /**
     * Verifies BlockFastProducerPolify.rejectedExecution() throws the proper exception when the
     * {@link java.util.concurrent.ThreadPoolExecutor} is shutdown.
     *
     * @param runnable {@link java.lang.Runnable} instance
     * @param executor {@link java.util.concurrent.ThreadPoolExecutor} instance
     */
    @Test
    public void testThrowsExceptionWhenExecutorIsShutdown(@Mocked final Runnable runnable,
                                                          @Mocked final ThreadPoolExecutor executor) {
        new Expectations() {
            {
                executor.isShutdown(); returns(true);
            }
        };

        final BlockFastProducerPolicy policy = new BlockFastProducerPolicy();

        exception.expect(RejectedExecutionException.class);
        policy.rejectedExecution(runnable, executor);
    }

    /**
     * Verifies BlockFastProducerPolify.rejectedExecution() throws the proper exception when the
     * {@link java.util.concurrent.ThreadPoolExecutor} is interrupted.
     *
     * @param runnable {@link java.lang.Runnable} instance
     * @param executor {@link java.util.concurrent.ThreadPoolExecutor} instance
     * @throws InterruptedException
     */
    @Test
    public void testThrowsExceptionWhenExecutorIsInterrupted(@Mocked final Runnable runnable,
                                                             @Mocked final ThreadPoolExecutor executor) throws InterruptedException {
        new Expectations() {
            {
                executor.isShutdown();
                returns(false);
                executor.getQueue().put(runnable);
                result = new InterruptedException("Expected Exception.");
            }
        };

        final BlockFastProducerPolicy policy = new BlockFastProducerPolicy();

        exception.expect(RejectedExecutionException.class);
        policy.rejectedExecution(runnable, executor);
    }

    @Test
    public void testCanPutRunnableCodeOnQueue(@Mocked final Runnable runnable,
                                              @Mocked final ThreadPoolExecutor executor) throws InterruptedException {
        final BlockFastProducerPolicy policy = new BlockFastProducerPolicy();
        policy.rejectedExecution(runnable, executor);

        new Verifications() {
            {
                executor.getQueue().put(runnable); times = 1;
            }
        };
    }
}
