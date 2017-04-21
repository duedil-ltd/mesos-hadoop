package com.duedil.mesos.java.executor.api;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.protobuf.ProtoHttpContent;
import com.google.protobuf.GeneratedMessage;
import org.apache.mesos.v1.Protos.ExecutorID;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.executor.Protos.Call;
import org.apache.mesos.v1.executor.Protos.Call.Builder;
import org.apache.mesos.v1.executor.Protos.Call.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.mesos.v1.executor.Protos.Call.Type.UPDATE;

public class UpdateRequest extends BaseRequest {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateRequest.class);

    private final Update update;
    private final FrameworkID frameworkId;
    private final ExecutorID executorId;

    public UpdateRequest(Update update, FrameworkID frameworkId, ExecutorID executorId, URI baseUrl) {
        super(baseUrl);
        this.update = checkNotNull(update);
        this.frameworkId = checkNotNull(frameworkId);
        this.executorId = checkNotNull(executorId);
    }

    @Override
    public HttpRequest createRequest() {
        Builder call = Call.newBuilder()
                .setType(UPDATE)
                .setUpdate(update)
                .setFrameworkId(frameworkId)
                .setExecutorId(executorId);

        HttpRequest request;
        try {
            request = REQUEST_FACTORY.buildPostRequest(baseUrl, new ProtoHttpContent(call.build()));
        } catch (IOException e) {
            LOG.error("Failed to build Update request: {}", e.getMessage());
            throw new RuntimeException(e);
        }

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
        return update;
    }

    @SuppressWarnings("LocalVariableOfConcreteClass")
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UpdateRequest request = (UpdateRequest) o;

        return frameworkId.equals(request.frameworkId) && executorId.equals(request.executorId) && baseUrl.equals(request.baseUrl);
    }

    @Override
    public int hashCode() {
        int result = frameworkId.hashCode();
        result = 31 * result + executorId.hashCode();
        result = 31 * result + baseUrl.hashCode();
        return result;
    }

}
