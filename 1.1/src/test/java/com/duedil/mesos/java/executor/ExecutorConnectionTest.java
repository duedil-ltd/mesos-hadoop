package com.duedil.mesos.java.executor;

import org.apache.mesos.v1.Protos.ExecutorID;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.junit.Before;
import org.junit.Test;

import java.net.URISyntaxException;

import static com.duedil.mesos.java.executor.ExecutorConnection.DEFAULT_MAX_RETRIES;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class ExecutorConnectionTest {

    private FrameworkID frameworkId;
    private ExecutorID executorId;
    private ActionableExecutorListener eventListener;

    @Before
    public void setUp() {
        frameworkId = FrameworkID.newBuilder().getDefaultInstanceForType();
        executorId = ExecutorID.newBuilder().getDefaultInstanceForType();
        eventListener = mock(ActionableExecutorListener.class);
    }

    @Test
    public void testRetainsValues() throws URISyntaxException {
        int maxRetries = 3;

        ExecutorConnection connection = new ExecutorConnection(frameworkId, executorId, eventListener, maxRetries);
        assertEquals(connection.getListener(), eventListener);
        assertEquals(connection.getFrameworkId(), frameworkId);
        assertEquals(connection.getExecutorId(), executorId);
        assertEquals(connection.getMaxRetries(), maxRetries);
    }

    @Test
    public void testUsesDefaultMaxRetries() throws URISyntaxException {
        ExecutorConnection connection = new ExecutorConnection(frameworkId, executorId, eventListener);
        assertEquals(connection.getMaxRetries(), DEFAULT_MAX_RETRIES);
    }

}
