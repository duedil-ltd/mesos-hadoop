package com.duedil.mesos.java.executor;

import com.google.protobuf.ByteString;
import org.apache.mesos.v1.Protos.TaskID;
import org.apache.mesos.v1.Protos.TaskInfo;
import org.apache.mesos.v1.executor.Protos.Call.Update;
import org.apache.mesos.v1.executor.Protos.Event;

import java.net.URI;
import java.util.Map;

public interface ActionableExecutorListener {

    /**
     * Called when an Executor Event is received.
     * @param event The Protobuf object representing the event received.
     */
    void onEvent(Event event);

    Map<TaskID, TaskInfo> getUnacknowledgedTasks();

    Map<ByteString, Update> getUnacknowledgedUpdates();

    URI getAgentEndpoint();

}
