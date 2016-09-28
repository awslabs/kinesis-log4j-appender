package com.amazonaws.services.kinesis.log4j.helpers;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests the {@link com.amazonaws.services.kinesis.log4j.helpers.Validator} class
 */
public class ValidatorTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    /**
     * Verifies Validator.isBlank() returns true when an empty string is passed.
     */
    @Test
    public void testValidatorProperlyDetectsEmptyString() {
        assertThat(Validator.isBlank(String.valueOf("")), is(true));
    }

    /**
     * Verifies Validator.isBlank() returns true when null is passed.
     */
    @Test
    public void testValidatorProperlyDetectsNullString() {
        assertThat(Validator.isBlank(null), is(true));
    }

    /**
     * Verifies Validator.isBlank() returns false when a string is passed.
     */
    @Test
    public void testValidatorProperlyDetectsValidString() {
        assertThat(Validator.isBlank(String.valueOf("Test String")), is(false));
    }

    /**
     * Verifies Validator.validate() throws an exception when it should.
     */
    @Test
    public void testValidatorThrowsException() {
        exception.expect(IllegalArgumentException.class);
        Validator.validate(false, "Expected Exception");
    }

    /**
     * Verifies Validator.validate() does not thrown an exception when it shouldn't.
     */
    @Test
    public void testValidatorDoesNotThrowAnException() {
        Validator.validate(true, "Exception should not be thrown.");
    }
}
