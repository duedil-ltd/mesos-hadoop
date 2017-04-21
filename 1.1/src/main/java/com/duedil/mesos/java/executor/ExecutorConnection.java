package com.duedil.mesos.java.executor;

import com.duedil.mesos.java.executor.api.SubscribeRequest;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.protobuf.util.JsonFormat;
import org.apache.mesos.v1.Protos.ExecutorID;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

import static com.google.api.client.util.Preconditions.checkNotNull;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.mesos.v1.executor.Protos.Call.Subscribe;
import static org.apache.mesos.v1.executor.Protos.Event;



public class ExecutorConnection extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorConnection.class);

    private final ActionableExecutorListener listener;
    private final FrameworkID frameworkId;
    private final ExecutorID executorId;
    private final URI agentEndpoint;
    private final int maxRetries;
    static final int DEFAULT_MAX_RETRIES = 5;
    private int retries = 0;

    ExecutorConnection(FrameworkID frameworkId, ExecutorID executorId, ActionableExecutorListener listener) {
        this(frameworkId, executorId, listener, DEFAULT_MAX_RETRIES);
    }

    ExecutorConnection(FrameworkID frameworkId, ExecutorID executorId, ActionableExecutorListener listener,
                       int maxRetries) {
        this.listener = checkNotNull(listener);
        this.agentEndpoint = listener.getAgentEndpoint();
        this.frameworkId = checkNotNull(frameworkId);
        this.executorId = checkNotNull(executorId);
        this.maxRetries = maxRetries;
    }

    @Override
    public void run() {
        // TODO: break out of this
        while (true) {
            Subscribe subscription = Subscribe.newBuilder()
                    .addAllUnacknowledgedTasks(listener.getUnacknowledgedTasks())
                    .addAllUnacknowledgedUpdates(listener.getUnacknowledgedUpdates())
                    .build();
            HttpRequest request = new SubscribeRequest(subscription, frameworkId, executorId, agentEndpoint).createRequest();

            try {
                HttpResponse response = request.execute();

                int statusCode = response.getStatusCode();
                if (statusCode == SC_NOT_FOUND) {
                    backoff();
                    continue;
                }
                else if (statusCode != SC_OK) {
                    throw new RuntimeException(String.format("Received bad response code: %d", statusCode));
                }
                resetRetries();

                try {
                    processResponseStream(response);
                }
                catch (IOException e) {
                    LOG.warn("Lost connection to agent: {}. Retrying...", e.getMessage());
                    backoff();
                }
            } catch (IOException e) {
                LOG.error("Error during SUBSCRIBE: {}", e.getMessage());
                backoff();
            }
        }
    }

    private void processResponseStream(HttpResponse response) throws IOException {
        try (InputStream content = response.getContent()) {
            BufferedReader r = new BufferedReader(new InputStreamReader(content));
            JsonFormat.Parser parser = JsonFormat.parser();

            while (true) {
                String lengthBytes;
                do {
                    lengthBytes = r.readLine();
                    if (lengthBytes == null) {
                        LOG.info("Stream terminated from remote end.");
                        return;
                    }
                } while (lengthBytes.length() < 1);

                int messageLength = Integer.valueOf(lengthBytes);
                char[] chars = new char[messageLength];
                int charsRead = r.read(chars, 0, messageLength);

                if (charsRead < messageLength) {
                    LOG.error("Incomplete message, expected {} bytes, read {} bytes", messageLength, charsRead);
                    throw new RuntimeException(String.format(
                            "Unable to read full message. Expected %d bytes, read %d bytes",
                            messageLength,
                            charsRead
                    ));
                }
                String message = String.valueOf(chars);

                Event.Builder builder = Event.newBuilder();
                parser.merge(message, builder);
                Event event = builder.build();
                listener.onEvent(event);
            }
        }
    }

    private void backoff() {
        if (++retries >= maxRetries) {
            LOG.error("Number of max retries ({}) exceeded, aborting", maxRetries);
            throw new RuntimeException(String.format("Backed off more than %d times, aborting.", maxRetries));
        }
        try {
            Thread.sleep((long)(Math.pow(2, retries) * 1000));
        } catch (InterruptedException e) {
            LOG.error("Backoff interrupted: {}", e.getMessage());
        }
    }

    private void resetRetries() {
        retries = 0;
    }

    ActionableExecutorListener getListener() {
        return listener;
    }

    FrameworkID getFrameworkId() {
        return frameworkId;
    }

    ExecutorID getExecutorId() {
        return executorId;
    }

    int getMaxRetries() {
        return maxRetries;
    }

}
