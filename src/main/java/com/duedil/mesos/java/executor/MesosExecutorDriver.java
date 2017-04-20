package com.duedil.mesos.java.executor;

import com.duedil.mesos.java.executor.api.MessageRequest;
import com.duedil.mesos.java.executor.api.Requestable;
import com.duedil.mesos.java.executor.api.UpdateRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.util.Base64;
import com.google.protobuf.ByteString;
import org.apache.mesos.v1.Protos.ExecutorID;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.FrameworkInfo;
import org.apache.mesos.v1.Protos.Status;
import org.apache.mesos.v1.Protos.TaskID;
import org.apache.mesos.v1.Protos.TaskInfo;
import org.apache.mesos.v1.Protos.TaskStatus;
import org.apache.mesos.v1.executor.Protos.Call.Message;
import org.apache.mesos.v1.executor.Protos.Call.Update;
import org.apache.mesos.v1.executor.Protos.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static com.duedil.mesos.java.Utils.executorEndpoint;
import static com.duedil.mesos.java.Utils.getEnv;
import static com.google.api.client.util.Preconditions.checkNotNull;
import static org.apache.http.HttpStatus.SC_ACCEPTED;

public class MesosExecutorDriver implements ExecutorDriver, ActionableExecutorListener {

    private static final Logger LOG = LoggerFactory.getLogger(MesosExecutorDriver.class);

    private static final String ENV_FRAMEWORK_ID = "MESOS_FRAMEWORK_ID";
    private static final String ENV_EXECUTOR_ID = "MESOS_EXECUTOR_ID";
    private static final String ENV_AGENT_ENDPOINT = "MESOS_AGENT_ENDPOINT";

    private final Executor executor;
    private FrameworkInfo framework;
    private final FrameworkID frameworkId;
    private final ExecutorID executorId;
    private ExecutorConnection conn;
    private final URI agentEndpoint;
    private final Map<TaskID, TaskInfo> unacknowledgedTasks;
    private final Map<ByteString, Update> unacknowledgedUpdates;

    MesosExecutorDriver(Executor executor) {
        this.executor = checkNotNull(executor);
        this.framework = null;
        this.frameworkId = FrameworkID.newBuilder().setValue(getEnv(ENV_FRAMEWORK_ID)).build();
        this.executorId  = ExecutorID.newBuilder().setValue(getEnv(ENV_EXECUTOR_ID)).build();
        this.conn = null;
        this.agentEndpoint = executorEndpoint(getEnv(ENV_AGENT_ENDPOINT));
        this.unacknowledgedTasks = new HashMap<>();
        this.unacknowledgedUpdates = new HashMap<>();
    }

    @Override
    public Status start() {
        conn = new ExecutorConnection(frameworkId, executorId, this);
        return Status.DRIVER_RUNNING;
    }

    @Override
    public Status stop() {
        // TODO: kill all the things
        return Status.DRIVER_STOPPED;
    }

    @Override
    public Status abort() {
        // TODO: kill all the things, perhaps differently
        return Status.DRIVER_ABORTED;
    }

    @Override
    public Status join() {
        try {
            conn.join();
        }
        catch (InterruptedException e) {
            LOG.error("Exception thrown waiting for connection join: {}", e.getMessage());
        }
        return Status.DRIVER_RUNNING;
    }

    @Override
    public Status run() {
        start();
        conn.run();
        return join();
    }

    @Override
    public Status sendStatusUpdate(TaskStatus status) {
        Update update = Update.newBuilder()
                .setStatus(checkNotNull(status))
                .build();

        unacknowledgedUpdates.put(status.getUuid(), update);

        Requestable request = new UpdateRequest(update, frameworkId, executorId, agentEndpoint);
        try {
            HttpResponse response = request.createRequest().execute();
            int statusCode = response.getStatusCode();
            if (statusCode != SC_ACCEPTED) {
                LOG.error("Failed to send status update, got status code {}", statusCode);
                // TODO: mesos-hadoop completely ignores the return value of this call, what do?
            }
        } catch (IOException e) {
            LOG.error("Error while sending update: {}", e.getMessage());
            backoff();
        }

        return Status.DRIVER_RUNNING;
    }

    @Override
    public Status sendFrameworkMessage(byte[] data) {
        // the data in the message should be base64-encoded, according to the API spec
        byte[] encodedData = Base64.encodeBase64(checkNotNull(data));
        ByteString payload = ByteString.copyFrom(encodedData);
        Message message = Message.newBuilder().setData(payload).build();

        Requestable request = new MessageRequest(message, frameworkId, executorId, agentEndpoint);
        try {
            HttpResponse response = request.createRequest().execute();
            int statusCode = response.getStatusCode();
            if (statusCode != SC_ACCEPTED) {
                LOG.error("Failed to send message, got status code {}", statusCode);
            }
        } catch (IOException e) {
            LOG.error("Error while sending message: {}", e.getMessage());
            backoff();
        }

        return Status.DRIVER_RUNNING;
    }

    @Override
    public void onEvent(Event event) {
        LOG.debug("Event: {}", event.getMessage());
        switch (event.getType()) {
            case LAUNCH_GROUP:
                break;
            case KILL:
                break;
            case ACKNOWLEDGED:
                break;
            case MESSAGE:
                break;
            case ERROR:
                break;
            case SHUTDOWN:
                break;
            case UNKNOWN:
                LOG.error("Unknown event: {}", event.toString());
                break;
            default:
                LOG.info("NOP event: {}", event.toString());
        }
    }

    private void backoff() {}

    Executor getExecutor() {
        return executor;
    }

    FrameworkID getFrameworkId() {
        return frameworkId;
    }

    ExecutorID getExecutorId() {
        return executorId;
    }

    ExecutorConnection getConnection() {
        return conn;
    }

    @Override
    public Map<TaskID, TaskInfo> getUnacknowledgedTasks() {
        return unacknowledgedTasks;
    }

    @Override
    public Map<ByteString, Update> getUnacknowledgedUpdates() {
        return unacknowledgedUpdates;
    }

    @Override
    public URI getAgentEndpoint() {
        return agentEndpoint;
    }

}
