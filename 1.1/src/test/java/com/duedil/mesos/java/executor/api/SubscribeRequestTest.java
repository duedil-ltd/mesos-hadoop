package com.duedil.mesos.java.executor.api;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import org.apache.mesos.v1.Protos.ExecutorID;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.executor.Protos.Call;
import org.apache.mesos.v1.executor.Protos.Call.Subscribe;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class SubscribeRequestTest {

    private Subscribe subscription;
    private FrameworkID frameworkId;
    private ExecutorID executorId;
    private URI baseUrl;

    @Before
    public void setUp() throws URISyntaxException {
        frameworkId = FrameworkID.newBuilder().setValue("fr4m3w0rk-1d").build();
        executorId = ExecutorID.newBuilder().setValue("3x3cut0r-1d").build();
        baseUrl = new URI("http://127.0.0.1:5050");

        subscription = Subscribe.newBuilder().build();
    }

    @Test
    public void testConstructorRetainsValues() {
        SubscribeRequest call = new SubscribeRequest(subscription, frameworkId, executorId, baseUrl);
        GenericUrl url = new GenericUrl(baseUrl);
        assertThat(call, is(not(nullValue())));

        assertEquals(call.getPayload(), subscription);
        assertThat(call.getFrameworkId(), is(equalTo(frameworkId)));
        assertThat(call.getExecutorId(), is(equalTo(executorId)));
        assertThat(call.getBaseUrl(), is(equalTo(url)));
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructorRequiresNonNullSubscription() {
        SubscribeRequest call = new SubscribeRequest(null, frameworkId, executorId, baseUrl);
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructorRequiresNonNullFrameworkId() {
        SubscribeRequest call = new SubscribeRequest(subscription, null, executorId, baseUrl);
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructorRequiresNonNullExecutorId() {
        SubscribeRequest call = new SubscribeRequest(subscription, frameworkId, null, baseUrl);
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructorRequiresNonNullBaseUrl() {
        SubscribeRequest call = new SubscribeRequest(subscription, frameworkId, executorId, null);
    }

    @Test
    public void testRequestHasCorrectHeaders() {
        Requestable call = new SubscribeRequest(subscription, frameworkId, executorId, baseUrl);
        HttpRequest req = call.createRequest();
        HttpHeaders headers = req.getHeaders();
        assertThat(headers.get("Content-type").toString(), is(equalTo("[application/json]")));
        assertThat(headers.get("Accept").toString(), is(equalTo("[application/json]")));
        assertThat(headers.get("Accept-encoding").toString(), is(equalTo("[chunked]")));
    }

    @Test
    public void testRequestHasCorrectContent() throws IOException {
        Requestable call = new SubscribeRequest(subscription, frameworkId, executorId, baseUrl);
        HttpRequest req = call.createRequest();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        req.getContent().writeTo(out);
        byte[] contentBytes = out.toByteArray();
        Call contentCall = Call.parseFrom(contentBytes);

        assertThat(contentCall.getFrameworkId(), is(equalTo(frameworkId)));
        assertThat(contentCall.getExecutorId(), is(equalTo(executorId)));
        Subscribe payload = contentCall.getSubscribe();
        assertThat(payload, is(equalTo(subscription)));
    }

}
