package com.duedil.mesos.executor;

import com.duedil.mesos.Utils.TimeConversion;
import com.duedil.mesos.executor.api.MessageRequest;
import com.duedil.mesos.executor.api.Requestable;
import com.duedil.mesos.executor.api.UpdateRequest;
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
import org.apache.mesos.v1.executor.Protos.Event.Error;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static com.duedil.mesos.Utils.executorEndpoint;
import static com.duedil.mesos.Utils.getEnvOrThrow;
import static com.google.api.client.util.Preconditions.checkNotNull;
import static org.apache.http.HttpStatus.SC_ACCEPTED;

public class MesosExecutorDriver implements ExecutorDriver, ActionableExecutorListener {

    private static final Logger LOG = LoggerFactory.getLogger(MesosExecutorDriver.class);

    private static final String ENV_FRAMEWORK_ID = "MESOS_FRAMEWORK_ID";
    private static final String ENV_EXECUTOR_ID = "MESOS_EXECUTOR_ID";
    private static final String ENV_AGENT_ENDPOINT = "MESOS_AGENT_ENDPOINT";
    private static final String ENV_EXECUTOR_SHUTDOWN_GRACE_PERIOD = "MESOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD";
    private static final String ENV_MESOS_CHECKPOINT = "MESOS_CHECKPOINT";
    private static final String ENV_MESOS_LOCAL = "MESOS_LOCAL";

    private final Executor executor;
    private FrameworkInfo framework;
    private final FrameworkID frameworkId;
    private final ExecutorID executorId;
    private ExecutorConnection conn;
    private final URI agentEndpoint;
    private final Map<TaskID, TaskInfo> unacknowledgedTasks;
    private final Map<ByteString, Update> unacknowledgedUpdates;
    private final boolean mesosCheckpoint;
    private final boolean mesosLocal;
    private final long shutdownGracePeriod;

    public MesosExecutorDriver(Executor executor) {
        this.executor = checkNotNull(executor);
        this.framework = null;
        this.frameworkId = FrameworkID.newBuilder().setValue(getEnvOrThrow(ENV_FRAMEWORK_ID)).build();
        this.executorId  = ExecutorID.newBuilder().setValue(getEnvOrThrow(ENV_EXECUTOR_ID)).build();
        this.conn = null;
        this.agentEndpoint = executorEndpoint(getEnvOrThrow(ENV_AGENT_ENDPOINT));
        this.unacknowledgedTasks = new HashMap<>();
        this.unacknowledgedUpdates = new HashMap<>();
        this.mesosCheckpoint = Boolean.valueOf(System.getenv(ENV_MESOS_CHECKPOINT));
        this.mesosLocal = Boolean.valueOf(System.getenv(ENV_MESOS_LOCAL));
        String shutdownGracePeriod = System.getenv(ENV_EXECUTOR_SHUTDOWN_GRACE_PERIOD);
        if (shutdownGracePeriod != null) {
            this.shutdownGracePeriod = TimeConversion.getInstance().parseToMillis(shutdownGracePeriod);
        }
        else {
            this.shutdownGracePeriod = 0;
        }
    }

    @Override
    public Status start() {
        conn = new ExecutorConnection(frameworkId, executorId, this);
        return Status.DRIVER_RUNNING;
    }

    @Override
    public Status stop() {
        for (TaskID task : unacknowledgedTasks.keySet()) {
            executor.killTask(this, task);
        }
        return Status.DRIVER_STOPPED;
    }

    @Override
    public Status abort() {
        for (TaskID task : unacknowledgedTasks.keySet()) {
            executor.killTask(this, task);
        }
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
        return Status.DRIVER_STOPPED;
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
            backoff(); // TODO: What do?
        }

        return Status.DRIVER_RUNNING;
    }

    @Override
    public void onEvent(Event event) {
        LOG.debug("Event: {}", event.getMessage());
        switch (event.getType()) {
            case SUBSCRIBED:
                onSubscribed(event);
                break;
            case LAUNCH:
                onLaunch(event);
                break;
            case LAUNCH_GROUP:
                // TODO: This is marked as *experimental* in the API doc, what do?
                break;
            case KILL:
                onKill(event);
                break;
            case ACKNOWLEDGED:
                onAcknowledged(event);
                break;
            case MESSAGE:
                onMessage(event);
                break;
            case SHUTDOWN:
                onShutdown(event);
                break;
            case ERROR:
                onError(event);
                break;
            case UNKNOWN:
                LOG.error("Unknown event: {}", event.toString());
                break;
            default:
                LOG.info("NOP event: {}", event.toString());
        }
    }

    private void onError(final Event event) {
        Error error = event.getError();
        LOG.error("Agent reported error '{}'", error.getMessage());
        Status status = abort();
        if (status != Status.DRIVER_ABORTED) {
            throw new RuntimeException("Driver failed to abort");
        }
        status = start();
        if (status != Status.DRIVER_RUNNING) {
            throw new RuntimeException("Driver failed to start");
        }
    }

    private void onShutdown(final Event event) {
        LOG.debug("Sending shutdown() request");
        executor.shutdown(this);
    }

    private void onMessage(final Event event) {
        Event.Message message = event.getMessage();
        byte[] bytes = Base64.decodeBase64(message.getData().toByteArray());
        String decodedMessage = new String(bytes);
        LOG.debug("Message received from agent: {}", decodedMessage);
    }

    private void onKill(final Event event) {
        TaskID taskId = event.getKill().getTaskId();
        LOG.debug("Sending killTask() request for task {}", taskId.toString());
        executor.killTask(this, taskId);
    }

    private void onLaunch(final Event event) {
        TaskInfo task = event.getLaunch().getTask();
        unacknowledgedTasks.put(task.getTaskId(), task);
        LOG.debug("Sending launchTask() request for task {}", task.toString());
        executor.launchTask(this, task);
    }

    private void onSubscribed(final Event event) {
        LOG.debug("Subscribed to agent");
        this.framework = event.getSubscribed().getFrameworkInfo();
    }

    private void onAcknowledged(final Event event) {
        TaskID taskId = event.getAcknowledged().getTaskId();
        ByteString uuid = event.getAcknowledged().getUuid();
        LOG.debug("Got ACKNOWLEDGE for taskId/uuid {}/{}", taskId.toString(), uuid.toString());
        unacknowledgedUpdates.remove(uuid);
        unacknowledgedTasks.remove(taskId);
    }

    private void backoff() {
        // TODO - placeholder
    }

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
    public Iterable<TaskInfo> getUnacknowledgedTasks() {
        return unacknowledgedTasks.values();
    }

    @Override
    public Iterable<Update> getUnacknowledgedUpdates() {
        return unacknowledgedUpdates.values();
    }

    @Override
    public URI getAgentEndpoint() {
        return agentEndpoint;
    }

}
