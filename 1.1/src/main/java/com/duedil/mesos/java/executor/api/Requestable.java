package com.duedil.mesos.java.executor.api;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.protobuf.GeneratedMessage;
import org.apache.mesos.v1.Protos.ExecutorID;
import org.apache.mesos.v1.Protos.FrameworkID;

public interface Requestable {

    HttpRequest createRequest();

    FrameworkID getFrameworkId();

    ExecutorID getExecutorId();

    GeneratedMessage getPayload();

    GenericUrl getBaseUrl();

}
