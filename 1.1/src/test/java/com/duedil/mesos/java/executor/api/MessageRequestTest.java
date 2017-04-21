package com.duedil.mesos.java.executor.api;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.util.Base64;
import com.google.protobuf.ByteString;
import org.apache.mesos.v1.Protos.ExecutorID;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.executor.Protos.Call;
import org.apache.mesos.v1.executor.Protos.Call.Message;
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

public class MessageRequestTest {

    private static final String MESSAGE_CONTENT_STRING = "this is a test of the emergency broadcast systems";
    private static final byte[] MESSAGE_CONTENT = MESSAGE_CONTENT_STRING.getBytes();
    private static final String FRAMEWORK_ID = "fr4m3w0rk-1d";
    private static final String EXECUTOR_ID = "3x3cut0r-1d";

    private Message message;
    private FrameworkID frameworkId;
    private ExecutorID executorId;
    private URI baseUrl;

    @Before
    public void setUp() throws URISyntaxException {
        baseUrl = new URI("http://127.0.0.1:5050/api/v1/executor");
        frameworkId = FrameworkID.newBuilder().setValue(FRAMEWORK_ID).build();
        executorId = ExecutorID.newBuilder().setValue(EXECUTOR_ID).build();

        ByteString data = ByteString.copyFrom(Base64.encodeBase64(MESSAGE_CONTENT));
        message = Message.newBuilder()
                .setData(data)
                .build();
    }

    @Test
    public void testConstructorRetainsValues() {
        MessageRequest request = new MessageRequest(message, frameworkId, executorId, baseUrl);
        GenericUrl url = new GenericUrl(baseUrl);
        assertThat(request, is(not(nullValue())));

        assertEquals(request.getPayload(), message);
        assertThat(request.getFrameworkId(), is(equalTo(frameworkId)));
        assertThat(request.getExecutorId(), is(equalTo(executorId)));
        assertThat(request.getBaseUrl(), is(equalTo(url)));
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructorRequiresNonNullMessage() {
        MessageRequest request = new MessageRequest(null, frameworkId, executorId, baseUrl);
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructorRequiresNonNullFrameworkId() {
        MessageRequest request = new MessageRequest(message, null, executorId, baseUrl);
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructorRequiresNonNullExecutorId() {
        MessageRequest request = new MessageRequest(message, frameworkId, null, baseUrl);
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructorRequiresNonNullBaseUrl() {
        MessageRequest request = new MessageRequest(message, frameworkId, executorId, null);
    }

    @Test
    public void testRequestHasCorrectHeaders() {
        Requestable call = new MessageRequest(message, frameworkId, executorId, baseUrl);
        HttpRequest req = call.createRequest();
        HttpHeaders headers = req.getHeaders();
        assertThat(headers.get("Content-type").toString(), is(equalTo("[application/json]"))); // WTF
        assertThat(headers.get("Accept").toString(), is(equalTo("[application/json]")));
    }

    @Test
    public void testRequestHasCorrectContent() throws IOException {
        Requestable call = new MessageRequest(message, frameworkId, executorId, baseUrl);
        HttpRequest req = call.createRequest();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        req.getContent().writeTo(out);
        byte[] contentBytes = out.toByteArray();
        Call contentCall = Call.parseFrom(contentBytes).toBuilder().build();

        assertThat(contentCall.getFrameworkId(), is(equalTo(frameworkId)));
        assertThat(contentCall.getExecutorId(), is(equalTo(executorId)));
        byte[] decodedData = Base64.decodeBase64(contentCall.getMessage().getData().toByteArray());
        String data = new String(decodedData);
        assertThat(data, is(equalTo(MESSAGE_CONTENT_STRING)));
    }
}
