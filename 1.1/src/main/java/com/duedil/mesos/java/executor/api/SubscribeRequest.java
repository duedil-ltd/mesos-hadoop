package com.duedil.mesos.java.executor.api;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.protobuf.ProtoHttpContent;
import com.google.protobuf.GeneratedMessage;
import org.apache.mesos.v1.Protos.ExecutorID;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.mesos.v1.executor.Protos.Call;
import static org.apache.mesos.v1.executor.Protos.Call.Builder;
import static org.apache.mesos.v1.executor.Protos.Call.Subscribe;
import static org.apache.mesos.v1.executor.Protos.Call.Type.SUBSCRIBE;

public class SubscribeRequest extends BaseRequest {

    private static final Logger LOG = LoggerFactory.getLogger(SubscribeRequest.class);

    private static final String ACCEPT_ENCODING = "chunked";

    private final Subscribe subscription;
    private final FrameworkID frameworkId;
    private final ExecutorID executorId;

    public SubscribeRequest(Subscribe subscription, FrameworkID frameworkId, ExecutorID executorId, URI baseUrl) {
        super(baseUrl);
        this.subscription = checkNotNull(subscription);
        this.frameworkId = checkNotNull(frameworkId);
        this.executorId = checkNotNull(executorId);
    }

    @Override
    public HttpRequest createRequest() {
        Builder call = Call.newBuilder()
                .setType(SUBSCRIBE)
                .setSubscribe(subscription)
                .setFrameworkId(frameworkId)
                .setExecutorId(executorId);

        HttpRequest request;
        try {
            request = REQUEST_FACTORY.buildPostRequest(baseUrl, new ProtoHttpContent(call.build()));
        } catch (IOException e) {
            LOG.error("Failed to build Subscribe request: {}", e.getMessage());
            throw new RuntimeException(e);
        }

        HttpHeaders newHeaders = request.getHeaders().clone();
        newHeaders.setAcceptEncoding(ACCEPT_ENCODING);
        request.setHeaders(newHeaders);
        request.setThrowExceptionOnExecuteError(false);

        return request;
    }

    @Override
    public FrameworkID getFrameworkId() {
        return frameworkId;
    }

    @Override
    public ExecutorID getExecutorId() {
        return executorId;
    }

    @Override
    public GeneratedMessage getPayload() {
        return subscription;
    }

    @SuppressWarnings("LocalVariableOfConcreteClass")
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SubscribeRequest that = (SubscribeRequest) o;

        return frameworkId.equals(that.frameworkId) && executorId.equals(that.executorId) && baseUrl.equals(that.baseUrl);
    }

    @Override
    public int hashCode() {
        int result = frameworkId.hashCode();
        result = 31 * result + executorId.hashCode();
        result = 31 * result + baseUrl.hashCode();
        return result;
    }

}
