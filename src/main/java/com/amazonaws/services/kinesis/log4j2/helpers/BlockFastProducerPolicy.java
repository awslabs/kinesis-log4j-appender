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
package com.amazonaws.services.kinesis.log4j2.helpers;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Policy that implements producer thread blocking indefinitely whenever a new
 * task cannot be scheduled in the threadpool for AWS SDK's async Kinesis
 * client. Using this policy, the produce thread will unblock only after there
 * is space in the threadpool's processing queue.
 */
public final class BlockFastProducerPolicy implements RejectedExecutionHandler {
  @Override
  public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
    if (executor.isShutdown()) {
      throw new RejectedExecutionException("Threadpoolexecutor already shutdown");
    } else {
      try {
        executor.getQueue().put(r);
      } catch (InterruptedException e) {
        throw new RejectedExecutionException(
            "Thread was interrupted while waiting for space to be available in the threadpool", e);
      }
    }
  }
}
