package com.duedil.mesos.java;

import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static com.duedil.mesos.java.Utils.executorEndpoint;
import static com.duedil.mesos.java.Utils.getEnv;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class UtilsTest {

    @Test
    public void testExecutorEndpointAppendsApiPath() throws URISyntaxException {
        String baseUrl = "http://127.0.0.1:5050";
        URI endpoint = executorEndpoint(baseUrl);
        assertThat(endpoint, is(equalTo(new URI(baseUrl + "/api/v1/executor"))));
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testExecutorEndpointThrowsExceptionWithNullInput() {
        URI endpoing = executorEndpoint(null);
    }

    @Test
    public void testGetEnvFetchesValueFromEnvironment() {
        String expected = "http://127.0.0.1:5050";
        String actual = getEnv("MESOS_AGENT_ENDPOINT");
        assertThat(actual, is(equalTo(expected)));
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testGetEnvThrowsNpeWhenEnvVarDoesntExist() {
        String actual = getEnv("MESOS_FROBNITZER");
    }

}
