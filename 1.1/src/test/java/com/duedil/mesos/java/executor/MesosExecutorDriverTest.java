package com.duedil.mesos.java.executor;

import org.apache.mesos.v1.Protos.ExecutorID;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class MesosExecutorDriverTest {

    private static final String FRAMEWORK_ID = "fr4m3w0rk-1d";
    private static final String EXECUTOR_ID = "3x3cut0r-1d";

    private Executor executor;
    private FrameworkID frameworkId;
    private ExecutorID executorId;

    @Before
    public void setUp() {
        executor = mock(Executor.class);

        frameworkId = FrameworkID.newBuilder().setValue(FRAMEWORK_ID).build();
        executorId = ExecutorID.newBuilder().setValue(EXECUTOR_ID).build();
    }

    @Test
    public void testConstructorRetainsValues() {
        MesosExecutorDriver driver = new MesosExecutorDriver(executor);
        assertEquals(driver.getExecutor(), executor);
        assertEquals(driver.getFrameworkId(), frameworkId);
        assertEquals(driver.getExecutorId(), executorId);
    }

    @Test
    public void testConstructorNullsConnection() {
        MesosExecutorDriver driver = new MesosExecutorDriver(executor);
        assertThat(driver.getConnection(), is(nullValue()));
    }

    @Test
    public void testConstructorSetsFrameworkId() {
        MesosExecutorDriver driver = new MesosExecutorDriver(executor);
        FrameworkID expectedFrameworkId = FrameworkID.newBuilder().setValue(FRAMEWORK_ID).build();
        assertEquals(driver.getFrameworkId(), expectedFrameworkId);
    }

}
