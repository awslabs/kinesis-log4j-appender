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

/**
 * Utility class for validating configurations
 */
public class Validator {
  /**
   * Tests if input is null or empty.
   * 
   * @param input
   *          test string
   * @return true if input is null or empty; false otherwise
   */
  public static boolean isBlank(String input) {
    return input == null || input.trim().isEmpty();
  }

  /**
   * For validating conditions and throwing error messages if the condition
   * doesn't hold true
   * 
   * @param trueCondition
   *          boolean condition to be validated
   * @param exceptionMsg
   *          error message to throw as {@code IllegalArgumentException} if
   *          condition doesn't hold true
   */
  public static void validate(boolean trueCondition, String exceptionMsg) {
    if (!trueCondition) {
      throw new IllegalArgumentException(exceptionMsg);
    }
  }
}
