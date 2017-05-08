package com.duedil.mesos.executor;

import org.apache.mesos.v1.Protos.TaskInfo;
import org.apache.mesos.v1.executor.Protos.Call.Update;
import org.apache.mesos.v1.executor.Protos.Event;

import java.net.URI;

public interface ActionableExecutorListener {

    /**
     * Called when an Executor Event is received.
     * @param event The Protobuf object representing the event received.
     */
    void onEvent(Event event);

    Iterable<TaskInfo> getUnacknowledgedTasks();

    Iterable<Update> getUnacknowledgedUpdates();

    URI getAgentEndpoint();

}
