package com.duedil.mesos.java.executor.api;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import org.apache.mesos.v1.Protos.ExecutorID;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.TaskID;
import org.apache.mesos.v1.Protos.TaskState;
import org.apache.mesos.v1.Protos.TaskStatus;
import org.apache.mesos.v1.executor.Protos.Call;
import org.apache.mesos.v1.executor.Protos.Call.Update;
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

public class UpdateRequestTest {

    private static final String FRAMEWORK_ID = "fr4m3w0rk-1d";
    private static final String EXECUTOR_ID = "3x3cut0r-1d";

    private Update update;
    private FrameworkID frameworkId;
    private ExecutorID executorId;
    private URI baseUrl;

    @Before
    public void setUp() throws URISyntaxException {
        frameworkId = FrameworkID.newBuilder().setValue(FRAMEWORK_ID).build();
        executorId = ExecutorID.newBuilder().setValue(EXECUTOR_ID).build();
        baseUrl = new URI("http://127.0.0.1:5050");

        update = Update.newBuilder()
                .setStatus(TaskStatus.newBuilder()
                        .setTaskId(TaskID.newBuilder().setValue("t4sk-1d").build())
                        .setState(TaskState.TASK_FINISHED)
                        .build())
                .build();
    }

    @Test
    public void testConstructorRetainsValues() {
        UpdateRequest call = new UpdateRequest(update, frameworkId, executorId, baseUrl);
        GenericUrl url = new GenericUrl(baseUrl);
        assertThat(call, is(not(nullValue())));

        assertEquals(call.getPayload(), update);
        assertThat(call.getFrameworkId(), is(equalTo(frameworkId)));
        assertThat(call.getExecutorId(), is(equalTo(executorId)));
        assertThat(call.getBaseUrl(), is(equalTo(url)));
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructorRequiresNonNullUpdate() {
        UpdateRequest call = new UpdateRequest(null, frameworkId, executorId, baseUrl);
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructorRequiresNonNullFrameworkId() {
        UpdateRequest call = new UpdateRequest(update, null, executorId, baseUrl);
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructorRequiresNonNullExecutorId() {
        UpdateRequest call = new UpdateRequest(update, frameworkId, null, baseUrl);
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructorRequiresNonNullBaseUrl() {
        UpdateRequest call = new UpdateRequest(update, frameworkId, executorId, null);
    }

    @Test
    public void testRequestHasCorrectHeaders() {
        Requestable call = new UpdateRequest(update, frameworkId, executorId, baseUrl);
        HttpRequest req = call.createRequest();
        HttpHeaders headers = req.getHeaders();
        assertThat(headers.get("Content-type").toString(), is(equalTo("[application/json]")));
        assertThat(headers.get("Accept").toString(), is(equalTo("[application/json]")));
    }

    @Test
    public void testRequestHasCorrectContent() throws IOException {
        Requestable call = new UpdateRequest(update, frameworkId, executorId, baseUrl);
        HttpRequest req = call.createRequest();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        req.getContent().writeTo(out);
        byte[] contentBytes = out.toByteArray();
        Call contentCall = Call.parseFrom(contentBytes);

        assertThat(contentCall.getFrameworkId(), is(equalTo(frameworkId)));
        assertThat(contentCall.getExecutorId(), is(equalTo(executorId)));
        Update payload = contentCall.getUpdate();
        assertThat(payload, is(equalTo(update)));
    }

}
