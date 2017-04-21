package com.duedil.mesos.java;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
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
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;

import org.apache.mesos.v1.Protos.AgentID;
import org.apache.mesos.v1.Protos.ExecutorID;
import org.apache.mesos.v1.Protos.Filters;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.FrameworkInfo;
import org.apache.mesos.v1.Protos.MasterInfo;
import org.apache.mesos.v1.Protos.Offer.Operation;
import org.apache.mesos.v1.Protos.Offer.Operation.Launch;
import org.apache.mesos.v1.Protos.OfferID;
import org.apache.mesos.v1.Protos.Request;
import org.apache.mesos.v1.Protos.TaskID;
import org.apache.mesos.v1.Protos.TaskInfo;
import org.apache.mesos.v1.Protos.TaskStatus;
import org.apache.mesos.v1.Protos.VersionInfo;
import org.apache.mesos.v1.scheduler.Protos.Call;
import org.apache.mesos.v1.scheduler.Protos.Call.Accept;
import org.apache.mesos.v1.scheduler.Protos.Call.Acknowledge;
import org.apache.mesos.v1.scheduler.Protos.Call.Decline;
import org.apache.mesos.v1.scheduler.Protos.Call.Reconcile;
import org.apache.mesos.v1.scheduler.Protos.Event;
import org.apache.mesos.v1.scheduler.Protos.Event.Error;
import org.apache.mesos.v1.scheduler.Protos.Event.Failure;
import org.apache.mesos.v1.scheduler.Protos.Event.Message;
import org.apache.mesos.v1.scheduler.Protos.Event.Offers;
import org.apache.mesos.v1.scheduler.Protos.Event.Rescind;
import org.apache.mesos.v1.scheduler.Protos.Event.Subscribed;
import org.apache.mesos.v1.scheduler.Protos.Event.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import static com.duedil.mesos.java.Utils.schedulerEndpoint;
import static com.duedil.mesos.java.Utils.versionEndpoint;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.http.HttpStatus.SC_MULTIPLE_CHOICES;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.mesos.v1.Protos.Offer.Operation.Type.LAUNCH;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.ACCEPT;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.ACKNOWLEDGE;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.DECLINE;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.KILL;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.MESSAGE;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.RECONCILE;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.REQUEST;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.REVIVE;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.SUPPRESS;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.TEARDOWN;

public class MesosSchedulerDriver implements SchedulerDriver, EventListener {

    private static final Logger LOG = LoggerFactory.getLogger(MesosSchedulerDriver.class);
    private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();

    private final Scheduler scheduler;
    private final FrameworkInfo framework;
    private final boolean implicitAcknowledgements;
    private boolean failover;
    private URI masterUri;
    private FrameworkID frameworkId;
    private String version;
    private SchedulerConnection conn;
    private String streamId = null;

    public MesosSchedulerDriver(Scheduler scheduler, FrameworkInfo framework, URI masterUri) {
        this(scheduler, framework, masterUri, false);
    }

    public MesosSchedulerDriver(Scheduler scheduler, FrameworkInfo framework, URI masterUri,
                                boolean implicitAcknowledgements) {
        this.scheduler = checkNotNull(scheduler);
        this.framework = addDefaultFrameworkValues(checkNotNull(framework));
        this.masterUri = checkNotNull(masterUri);
        this.implicitAcknowledgements = checkNotNull(implicitAcknowledgements);
        this.version = null;
        this.failover = false;
        this.conn = null;
    }

    @Override
    public void start() {
        // TODO handle zk URIs
        // TODO add standard port if URI lacks a port
        // TODO can this be initialised in constructor?
        conn = new SchedulerConnection(framework, this, masterUri, frameworkId);
    }

    // TODO abort?

    public void join() {
        try {
            conn.join();
        } catch (InterruptedException e) {
            LOG.error("Exception thrown waiting for connection join", e);
        }
    }

    public void run() {
        start();
        conn.run();
        join();
    }


    private FrameworkInfo addDefaultFrameworkValues(FrameworkInfo framework) {
        FrameworkInfo.Builder frameworkInfo = framework.toBuilder();

        if (!frameworkInfo.hasFailoverTimeout()) {
            frameworkInfo.setFailoverTimeout(100.0);
        }

        return frameworkInfo.build();
    }

    public void stop() {
        stop(false);
    }

    @Override
    public void stop(boolean failover) {
        // lock
            this.failover = failover;
            // grab detector, set class level detector to null

        // if there was a detector, stop it

        shutDown();
    }

    @Override
    public void changeMaster(URI newMaster) {
        // TODO why the version request here?
        // TODO what is version used for?
        checkNotNull(newMaster);
        version = getVersion(newMaster);
        this.masterUri = newMaster;
    }

    public String getVersion(URI master) {
        if (master != null) {
            HttpRequestFactory requestFactory = HTTP_TRANSPORT.createRequestFactory(
                    new HttpRequestInitializer() {
                        @Override
                        public void initialize(HttpRequest request) {
                            request.setParser(new ProtoObjectParser());
                        }
                    });
            LOG.debug("Checking version at " + versionEndpoint(master));
            GenericUrl url = new GenericUrl(versionEndpoint(master));
            try {
                HttpRequest request = requestFactory.buildGetRequest(url);
                HttpResponse response = request.execute();
                if (response.getStatusCode() != SC_OK) {
                    LOG.error(String.format(
                            "Unable to fetch version, response code %d, Message: %s",
                            response.getStatusCode(),
                            response.getStatusMessage()));
                    return null;
                }
                String versionJson = response.parseAsString();
                JsonFormat.Parser parser = JsonFormat.parser();
                VersionInfo.Builder versionInfo = VersionInfo.newBuilder();
                parser.merge(versionJson, versionInfo);
                return versionInfo.build().getVersion();
            } catch (HttpResponseException e) {
                // TODO handle this appropriately
                LOG.error("Received bad response when fetching version", e);
            } catch(IOException e) {
                throw new RuntimeException(e);
            }
        }
        // TODO should throw exception here?
        return "";
    }

    private void shutDown() {
        if (!failover) {
            tearDown();
        }
    }

    private boolean connected() {
        return streamId != null;
    }

    private void tearDown() {
        if (connected()) {
            if (frameworkId != null) {
                Call teardown = Call.newBuilder()
                        .setType(TEARDOWN)
                        .setFrameworkId(frameworkId)
                        .build();
                send(teardown);
                conn.shutdown();
                // :(
                frameworkId = null;
            }
        }
    }

    private void onNewMasterDetectedMessage(ByteString data) {
        URI master = null;
//        // is this a FrameworkInfo? URL?
//        URL url = parseFrom(data); // ????
//        if (url.hasAddress()) {
//            master = String.format("%s:%d", url.getAddress().getIp(), url.getAddress().getPort());
//        }

        if (master != null) {
            changeMaster(master);
        }
    }

    private void onNoMasterDetectedMessage() {
        changeMaster(null);
    }

    @Override
    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    private void send(Call body) {
        // TODO lock?
        checkNotNull(body);
        if (!connected()) {
            throw new RuntimeException("Must subscribe Framework before trying to call methods");
        }

        HttpRequestFactory requestFactory = HTTP_TRANSPORT.createRequestFactory(
                new HttpRequestInitializer() {
                    @Override
                    public void initialize(HttpRequest request) {
                        request.setParser(new ProtoObjectParser());
                        HttpHeaders headers = new HttpHeaders();
                        headers.set("Mesos-Stream-Id", streamId);
                        headers.setContentType("application/json");
                        headers.setAccept("application/json");
                        request.setHeaders(headers);
                    }
                });

        GenericUrl url = new GenericUrl(schedulerEndpoint(this.masterUri));
        HttpContent content = new ProtoHttpContent(body);
        try {
            HttpRequest request = requestFactory.buildPostRequest(url, content);
            HttpResponse response = request.execute();
            if (response.getStatusCode() < SC_OK || response.getStatusCode() >= SC_MULTIPLE_CHOICES) {
                throw new RuntimeException(String.format(
                        "Failed to send request (%d): %s\n%s",
                        response.getStatusCode(),
                        response.parseAsString(),
                        body.toString()));
            }
        } catch (IOException e) {
            throw new RuntimeException("IOException: ABORT ABORT ABORT", e);
        }
    }

    @Override
    public void requestResources(List<Request> requests) {
        checkNotNull(frameworkId);
        checkNotNull(requests);

        Call.Request.Builder request = Call.Request.newBuilder()
                .addAllRequests(requests);

        Call resReq = Call.newBuilder()
                .setType(REQUEST)
                .setRequest(request)
                .setFrameworkId(frameworkId)
                .build();
        send(resReq);
    }

    @Override
    public void acceptOffers(List<OfferID> offerIds, List<Operation> operations, Filters filters) {
        if (operations == null || operations.isEmpty()) {
            declineOffers(offerIds, filters);
            return;
        }

        checkNotNull(frameworkId);
        checkNotNull(offerIds);

        Accept.Builder accept = Accept.newBuilder()
                .addAllOfferIds(offerIds)
                .addAllOperations(operations);

        if (filters != null) {
            accept.setFilters(filters);
        }

        Call call = Call.newBuilder()
                .setType(ACCEPT)
                .setAccept(accept)
                .setFrameworkId(frameworkId)
                .build();
        send(call);
    }

    @Override
    public void launchTasks(List<OfferID> offerIds, List<TaskInfo> tasks, Filters filters) {
        if (tasks == null || tasks.isEmpty()) {
            declineOffers(offerIds, filters);
            return;
        }
        checkNotNull(frameworkId);
        checkNotNull(offerIds);

        Operation launch = Operation.newBuilder()
                .setType(LAUNCH)
                .setLaunch(Launch.newBuilder()
                        .addAllTaskInfos(tasks)
                )
                .build();

        List<Operation> operations = Arrays.asList(launch);

        acceptOffers(offerIds, operations, filters);
    }

    @Override
    public void declineOffers(List<OfferID> offerIds, Filters filters) {
        checkNotNull(frameworkId);
        checkNotNull(offerIds);

        Decline.Builder decline = Decline.newBuilder()
                .addAllOfferIds(offerIds);

        if (filters != null) {
            decline.setFilters(filters);
        }

        Call call = Call.newBuilder()
                .setType(DECLINE)
                .setDecline(decline)
                .setFrameworkId(frameworkId)
                .build();
        send(call);
    }

    @Override
    public void reviveOffers() {
        checkNotNull(frameworkId);

        Call revive = Call.newBuilder()
                .setType(REVIVE)
                .setFrameworkId(frameworkId)
                .build();
        send(revive);
    }

    @Override
    public void suppressOffers() {
        checkNotNull(frameworkId);

        Call suppress = Call.newBuilder()
                .setType(SUPPRESS)
                .setFrameworkId(frameworkId)
                .build();
        send(suppress);
    }

    @Override
    public void killTask(TaskID taskId) {
        checkNotNull(frameworkId);

        Call kill = Call.newBuilder()
                .setType(KILL)
                .setFrameworkId(frameworkId)
                .build();
        send(kill);
    }

    @Override
    public void acknowledgeStatusUpdate(TaskStatus status) {
        checkNotNull(frameworkId);
        checkNotNull(status);

        Acknowledge.Builder acknowledge = Acknowledge.newBuilder()
                .setAgentId(status.getAgentId())
                .setTaskId(status.getTaskId())
                .setUuid(status.getUuid());

        Call call = Call.newBuilder()
                .setType(ACKNOWLEDGE)
                .setAcknowledge(acknowledge)
                .setFrameworkId(frameworkId)
                .build();

        send(call);
    }

    @Override
    public void sendFrameworkMessage(ExecutorID executorId, AgentID agentId, ByteString data) {
        checkNotNull(frameworkId);
        checkNotNull(executorId);
        checkNotNull(agentId);

        Call.Message.Builder message = Call.Message.newBuilder()
                .setAgentId(agentId)
                .setExecutorId(executorId)
                .setData(data);

        Call call = Call.newBuilder()
                .setType(MESSAGE)
                .setMessage(message)
                .setFrameworkId(frameworkId)
                .build();
        send(call);
    }

    @Override
    public void reconcileTasks(List<TaskInfo> tasks) {
        checkNotNull(frameworkId);

        Reconcile.Builder reconcile = Reconcile.newBuilder();

        Call call = Call.newBuilder()
                .setType(RECONCILE)
                .setReconcile(reconcile)
                .setFrameworkId(frameworkId)
                .build();
        send(call);
    }

    private void onSubscribed(Subscribed subscribed) {
        boolean reregistered = this.frameworkId != null;
        frameworkId = subscribed.getFrameworkId();
        MasterInfo masterInfo = subscribed.getMasterInfo();

        if (version != null) {
            masterInfo = masterInfo.toBuilder()
                    .setVersion(version)
                    .build();
        }

        if (reregistered) {
            scheduler.reregistered(this, masterInfo);
        } else {
            scheduler.registered(this, frameworkId, masterInfo);
        }
    }

    // TODO what calls this? called when master changed, when connection lost, on shutdown
    private void onClose() {
        scheduler.disconnected(this);
    }

    private void onOffers(Offers offers) {
        scheduler.resourceOffers(this, offers.getOffersList());
    }

    private void onRescind(Rescind rescind) {
        scheduler.offerRescinded(this, rescind.getOfferId());
    }

    private void onUpdate(Update update) {
        scheduler.statusUpdate(this, update.getStatus());
        if (implicitAcknowledgements) {
            acknowledgeStatusUpdate(update.getStatus());
        }
    }

    private void onMessage(Message message) {
        scheduler.frameworkMessage(
                this,
                message.getExecutorId(),
                message.getAgentId(),
                message.getData());
    }

    private void onFailure(Failure failure) {
        if (failure.hasExecutorId()) {
            scheduler.executorLost(
                    this,
                    failure.getExecutorId(),
                    failure.getAgentId(),
                    failure.getStatus());
        } else {
            scheduler.agentLost(this, failure.getAgentId());
        }
    }

    private void onError(Error error) {
        scheduler.error(this, error.getMessage());
    }

    @Override
    public void onEvent(Event event) {
        switch (event.getType()) {
            case SUBSCRIBED:
                onSubscribed(event.getSubscribed());
                break;
            case OFFERS:
                onOffers(event.getOffers());
                break;
            case RESCIND:
                onRescind(event.getRescind());
                break;
            case UPDATE:
                onUpdate(event.getUpdate());
                break;
            case MESSAGE:
                onMessage(event.getMessage());
                break;
            case FAILURE:
                onFailure(event.getFailure());
                break;
            case ERROR:
                onError(event.getError());
                break;
            case HEARTBEAT:
                LOG.debug(String.format("Heartbeat: %s", event.toString()));
                break;
            case INVERSE_OFFERS:
                LOG.error(String.format("Inverse Offer: %s", event.toString()));
                break;
            case RESCIND_INVERSE_OFFER:
                LOG.error(String.format("Rescind Inverse Offer: %s", event.toString()));
                break;
            case UNKNOWN:
            default:
                LOG.error(String.format("Unknown event: %s", event.toString()));
        }
    }
}
