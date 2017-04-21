package com.duedil.mesos.java;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.http.protobuf.ProtoHttpContent;
import com.google.api.client.protobuf.ProtoObjectParser;
import com.google.protobuf.util.JsonFormat;

import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.FrameworkInfo;
import org.apache.mesos.v1.scheduler.Protos.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

import static com.duedil.mesos.java.Utils.schedulerEndpoint;
import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.HttpStatus.SC_TEMPORARY_REDIRECT;
import static org.apache.mesos.v1.scheduler.Protos.Call;
import static org.apache.mesos.v1.scheduler.Protos.Call.Subscribe;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.SUBSCRIBE;

public class SchedulerConnection extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(SchedulerConnection.class);
    private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();

    private final FrameworkInfo framework;
    private final EventListener listener;
    private final FrameworkID frameworkId;
    private final int maxRetries;
    private int retries = 0;
    private URI masterUri;
    private boolean shutdown = false;

    public SchedulerConnection(FrameworkInfo framework, EventListener listener, URI masterUri,
                               FrameworkID frameworkId) {
        this(framework, listener, masterUri, frameworkId, 5);
    }

    public SchedulerConnection(FrameworkInfo framework, EventListener listener, URI masterUri,
                               FrameworkID frameworkId, int maxRetries) {
        this.framework = checkNotNull(framework);
        this.listener = checkNotNull(listener);
        this.masterUri = checkNotNull(masterUri);
        this.frameworkId = frameworkId;
        this.maxRetries = maxRetries;
    }

    @Override
    public void run() {
        // TODO need an escape from this loop to shut the scheduler driver down
        while (true) {
            HttpRequest request = createSubscribeRequest();
            try {
                HttpResponse response = request.execute();

                if (response.getStatusCode() == SC_TEMPORARY_REDIRECT) {
                    masterUri = parseNewMaster(response.getHeaders().getFirstHeaderStringValue("Location"));
                    listener.changeMaster(masterUri);
                    LOG.debug("Redirect received, updating master to " + masterUri.toString());
                    backOff();
                    continue;
                } else if (response.getStatusCode() == SC_NOT_FOUND) {
                    backOff();
                    continue;
                } else if (response.getStatusCode() != SC_OK) {
                    throw new RuntimeException(String.format("Received bad response code: %d", response.getStatusCode()));
                }
                resetRetries();

                listener.setStreamId(response.getHeaders().getFirstHeaderStringValue("Mesos-Stream-Id"));
                try {
                    processResponseStream(response);
                    if (shutdown) {
                        break;
                    }
                } catch (IOException e) {
                    LOG.warn(String.format("Lost connection to master: %s. Retrying...", e.getMessage()));
                    backOff();
                }
            } catch (HttpResponseException e) {
                // TODO will this still occur?
                throw new RuntimeException(String.format("Error subscribing to Mesos at %s: %d, %s", masterUri.toString(), e.getStatusCode(), e.getStatusMessage()), e);
            } catch (IOException e) {
                // TODO don't log full exception here?
                LOG.error("Error subscribing to master", e);
                backOff();
            }
        }
    }

    public void shutdown() {
        shutdown = true;
    }

    private URI parseNewMaster(String location) {
        return URI.create(String.format("http:%s", location.substring(0, location.length() - 24)));
    }

    private void resetRetries() {
        retries = 0;
    }

    private void backOff() {
        if (++retries >= maxRetries) {
            throw new RuntimeException(String.format("Backed off more than %d times, aborting", maxRetries));
        }
        try {
            Thread.sleep((long) (Math.pow(2, retries) * 1000));
        } catch (InterruptedException e) {
            LOG.error("Backoff interrupted", e);
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
                    throw new RuntimeException(String.format(
                            "Unable to read full message. Expected %d bytes, read %d bytes",
                            messageLength,
                            charsRead));
                }
                String message = String.valueOf(chars);

                Event.Builder builder = Event.newBuilder();
                parser.merge(message, builder);
                Event event = builder.build();
                listener.onEvent(event);
            }
        }
    }

    private HttpRequest createSubscribeRequest() {
        HttpRequest request;Subscribe.Builder subscribe = Subscribe.newBuilder()
                .setFrameworkInfo(framework);

        Call.Builder subCall = Call.newBuilder()
                .setType(SUBSCRIBE)
                .setSubscribe(subscribe);

        if (frameworkId != null) {
            subCall.setFrameworkId(frameworkId);
        }


        HttpRequestFactory requestFactory = HTTP_TRANSPORT.createRequestFactory(
                new HttpRequestInitializer() {
                    @Override
                    public void initialize(HttpRequest request) {
                        request.setParser(new ProtoObjectParser());
                        HttpHeaders headers = new HttpHeaders();
                        headers.setContentType("application/json");
                        headers.setAccept("application/json");
                        request.setHeaders(headers);
                    }
                });

        try {
            GenericUrl url = new GenericUrl(schedulerEndpoint(this.masterUri));
            request = requestFactory.buildPostRequest(url, new ProtoHttpContent(subCall.build()));
        } catch (IOException e) {
            throw new RuntimeException("Error building Subscribe request", e);
        }

        request.setThrowExceptionOnExecuteError(false);
        return request;
    }
}
