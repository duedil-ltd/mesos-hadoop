package com.duedil.mesos.java;

import org.apache.mesos.v1.scheduler.Protos.Event;

import java.net.URI;

// TODO rename?
public interface EventListener {

    /**
     * Called when a Scheduler Event is received.
     * @param event The Protobuf object representing the event received.
     */
    void onEvent(Event event);

    // TODO javadoc
    void setStreamId(String streamId);

    void changeMaster(URI newMaster);
}
