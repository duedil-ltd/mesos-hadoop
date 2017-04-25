package com.duedil.mesos;

import com.duedil.mesos.Utils.TimeConversion;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static com.duedil.mesos.Utils.executorEndpoint;
import static com.duedil.mesos.Utils.getEnvOrThrow;
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
        String actual = getEnvOrThrow("MESOS_AGENT_ENDPOINT");
        assertThat(actual, is(equalTo(expected)));
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testGetEnvThrowsNpeWhenEnvVarDoesntExist() {
        String actual = getEnvOrThrow("MESOS_FROBNITZER");
    }

    @Test
    public void testTimeConversionHoldsAllUnits() {
        TimeConversion tc = TimeConversion.getInstance();

        long oneMili = 1;
        long oneSec = oneMili * 1000;
        long oneMin = oneSec * 60;
        long oneHour = oneMin * 60;
        long oneDay = oneHour * 24;
        long oneWeek = oneDay * 7;

        assertThat(tc.parseToMillis("1ms"), is(equalTo(oneMili)));
        assertThat(tc.parseToMillis("1secs"), is(equalTo(oneSec)));
        assertThat(tc.parseToMillis("1mins"), is(equalTo(oneMin)));
        assertThat(tc.parseToMillis("1hrs"), is(equalTo(oneHour)));
        assertThat(tc.parseToMillis("1days"), is(equalTo(oneDay)));
        assertThat(tc.parseToMillis("1weeks"), is(equalTo(oneWeek)));
    }

    @Test
    public void testTimeConversionIsCaseInsensitive() {
        TimeConversion tc = TimeConversion.getInstance();

        long oneMili = 1;
        long oneSec = oneMili * 1000;
        long oneMin = oneSec * 60;
        long oneHour = oneMin * 60;
        long oneDay = oneHour * 24;
        long oneWeek = oneDay * 7;

        assertThat(tc.parseToMillis("1MS"), is(equalTo(oneMili)));
        assertThat(tc.parseToMillis("1SECS"), is(equalTo(oneSec)));
        assertThat(tc.parseToMillis("1MINS"), is(equalTo(oneMin)));
        assertThat(tc.parseToMillis("1HRS"), is(equalTo(oneHour)));
        assertThat(tc.parseToMillis("1DAYS"), is(equalTo(oneDay)));
        assertThat(tc.parseToMillis("1WEEKS"), is(equalTo(oneWeek)));
    }

}
