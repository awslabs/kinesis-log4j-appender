package com.amazonaws.services.kinesis.log4j.helpers;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import mockit.Mock;
import mockit.MockUp;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Tests the {@link com.amazonaws.services.kinesis.log4j.helpers.CustomCredentialsProviderChain} class
 *
 * NOTE: These tests primarily exercise internals of the AWS SDK and would probably be better served as
 *       Integration tests that test actual credentials. But these help Code Coverage metrics for now.
 */
public class CustomCredentialsProviderChainTest {

    public static final String ACCESS_KEY_ID = "ACCESS_KEY_ID";
    public static final String SECRET_KEY = "SECRET_KEY";

    @BeforeClass
    public static void setUp() {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.ERROR);
    }

    /**
     * Verify the provider chain can load credentials from system properties
     */
    @Test
    public void testCanLoadAwsCredentialsFromSystemProperties() {
        Properties properties = System.getProperties();
        properties.setProperty("aws.accessKeyId", ACCESS_KEY_ID);
        properties.setProperty("aws.secretKey", SECRET_KEY);

        CustomCredentialsProviderChain chain = new CustomCredentialsProviderChain();
        AWSCredentials credentials = chain.getCredentials();

        assertThat(credentials, is(notNullValue()));
        assertThat(credentials.getAWSAccessKeyId(), is(equalTo(ACCESS_KEY_ID)));
        assertThat(credentials.getAWSSecretKey(), is(equalTo(SECRET_KEY)));
    }

    /**
     * Verify the provider chain can load credentials from environment variables.
     */
    @Test
    public void textCanLoadAwsCredentialsFromEnvironmentVariables() {
        Map<String, String> environmentVariables = new HashMap<>(2);
        environmentVariables.put("AWS_ACCESS_KEY_ID", ACCESS_KEY_ID);
        environmentVariables.put("AWS_SECRET_KEY", SECRET_KEY);
        setEnvironmentVariables(environmentVariables);

        CustomCredentialsProviderChain chain = new CustomCredentialsProviderChain();
        AWSCredentials credentials = chain.getCredentials();

        assertThat(credentials, is(notNullValue()));
        assertThat(credentials.getAWSAccessKeyId(), is(equalTo(ACCESS_KEY_ID)));
        assertThat(credentials.getAWSSecretKey(), is(equalTo(SECRET_KEY)));
    }

    /**
     * Verify the provider chain can load credentials from an AWS instance
     */
    @Test
    public void testCanLoadAwsCredentialsFromAwsInstanceProfile() {

        new MockUp<InstanceProfileCredentialsProvider>() {
            @SuppressWarnings("unused")
            @Mock protected boolean needsToLoadCredentials() { return false; }
            @SuppressWarnings("unused")
            @Mock private boolean expired() { return false; }
            @SuppressWarnings("unused")
            @Mock public AWSCredentials getCredentials() {
                return new AWSCredentials() {
                    @Override
                    public String getAWSAccessKeyId() {
                        return ACCESS_KEY_ID;
                    }

                    @Override
                    public String getAWSSecretKey() {
                        return SECRET_KEY;
                    }
                };
            }
        };

        CustomCredentialsProviderChain chain = new CustomCredentialsProviderChain();
        AWSCredentials credentials = chain.getCredentials();

        assertThat(credentials, is(notNullValue()));
        assertThat(credentials.getAWSAccessKeyId(), is(equalTo(ACCESS_KEY_ID)));
        assertThat(credentials.getAWSSecretKey(), is(equalTo(SECRET_KEY)));
    }

    /**
     * Verify the provider chain can load credentials from the properties file in the classpath
     */
    @Test
    public void testCanLoadAwsCredentialsFromClassPathPropertiesFile() {
        CustomCredentialsProviderChain chain = new CustomCredentialsProviderChain();
        AWSCredentials credentials = chain.getCredentials();

        assertThat(credentials, is(notNullValue()));
        assertThat(credentials.getAWSAccessKeyId(), is(equalTo(ACCESS_KEY_ID)));
        assertThat(credentials.getAWSSecretKey(), is(equalTo(SECRET_KEY)));
    }

    // override the environment variables in memory with the passed map on *nix/Windows
    // See the excellent example on <a href="http://stackoverflow.com/a/7201825">stackoverflow</a>.
    private void setEnvironmentVariables(Map<String, String> environmentVariables) {
        try
        {
            Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
            Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
            theEnvironmentField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
            env.putAll(environmentVariables);
            Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
            theCaseInsensitiveEnvironmentField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, String> cienv = (Map<String, String>)     theCaseInsensitiveEnvironmentField.get(null);
            cienv.putAll(environmentVariables);
        }
        catch (NoSuchFieldException e)
        {
            try {
                Class[] classes = Collections.class.getDeclaredClasses();
                Map<String, String> env = System.getenv();
                for(Class cl : classes) {
                    if("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
                        Field field = cl.getDeclaredField("m");
                        field.setAccessible(true);
                        Object obj = field.get(env);
                        @SuppressWarnings("unchecked")
                        Map<String, String> map = (Map<String, String>) obj;
                        map.clear();
                        map.putAll(environmentVariables);
                    }
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }
}
