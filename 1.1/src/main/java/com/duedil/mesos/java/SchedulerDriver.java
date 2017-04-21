package com.duedil.mesos.java;

import com.google.protobuf.ByteString;

import org.apache.mesos.v1.Protos.AgentID;
import org.apache.mesos.v1.Protos.ExecutorID;
import org.apache.mesos.v1.Protos.Filters;
import org.apache.mesos.v1.Protos.Offer.Operation;
import org.apache.mesos.v1.Protos.OfferID;
import org.apache.mesos.v1.Protos.Request;
import org.apache.mesos.v1.Protos.TaskStatus;
import org.apache.mesos.v1.Protos.TaskInfo;
import org.apache.mesos.v1.Protos.TaskID;

import java.util.List;

/**
 * Interface for Mesos scheduler drivers. Users may wish to implement this class in mock
 * objects for tests.
 */
public interface SchedulerDriver {
        
    /**
     * Starts the scheduler driver.  This needs to be called before any other driver calls
     * are made.
     */
    void start();

    /**
     * Stops the scheduler driver. If the 'failover' flag is set to False then it is expected
     * that this framework will never reconnect to Mesos and all of its executors and tasks
     * can be terminated.  Otherwise, all executors and tasks will remain running (for some
     * framework specific failover timeout) allowing the scheduler to reconnect (possibly in
     * the same process, or from a different process, for example, on a different machine.)
     */
    void stop(boolean failover);

//    /**
//     * Aborts the driver so that no more callbacks can be made to the scheduler.  The semantics
//     * of abort and stop have deliberately been separated so that code can detect an aborted
//     * driver (i.e., via the return status of SchedulerDriver.join), and instantiate and start
//     * another driver if desired (from within the same process.)
//     */
//    void abort();

//    /**
//     * Waits for the driver to be stopped or aborted, possibly blocking the current thread
//     * indefinitely.  The return status of this function can be used to determine if the driver
//     * was aborted (see mesos.proto for a description of Status).
//     */
//    void join();

//    /**
//     * Starts and immediately joins (i.e., blocks on) the driver.
//     */
//    void run();

    /**
     * Requests resources from Mesos (see mesos.proto for a description of Request and how,
     * for example, to request resources from specific slaves.)  Any resources available are
     * offered to the framework via Scheduler.resourceOffers callback, asynchronously.
     */
    void requestResources(List<Request> requests);

    /**
     * Launches the given set of tasks. Any remaining resources (i.e., those that are not used
     * by the launched tasks or their executors) will be considered declined. Note that this
     * includes resources used by tasks that the framework attempted to launch but failed
     * (with TASK_ERROR) due to a malformed task description. The specified filters are applied
     * on all unused resources (see mesos.proto for a description of Filters). Available
     * resources are aggregated when multiple offers are provided. Note that all offers must
     * belong to the same slave. Invoking this function with an empty collection of tasks
     * declines offers in their entirety (see Scheduler.declineOffer). Note that passing a
     * single offer is also supported.
     */
    void launchTasks(List<OfferID> offerIds, List<TaskInfo> tasks, Filters filters);

    /**
     *  Kills the specified task. Note that attempting to kill a task is currently not
     *  reliable.  If, for example, a scheduler fails over while it was attempting to kill
     *  a task it will need to retry in the future. Likewise, if unregistered / disconnected,
     *  the request will be dropped (these semantics may be changed in the future).
     */
    void killTask(TaskID taskId);

    /**\
     *  Accepts the given offers and performs a sequence of operations on those accepted
     *  offers. See Offer.Operation in mesos.proto for the set of available operations. Any
     *  remaining resources (i.e., those that are not used by the launched tasks or their
     *  executors) will be considered declined. Note that this includes resources used by
     *  tasks that the framework attempted to launch but failed (with TASK_ERROR) due to a
     *  malformed task description. The specified filters are applied on all unused resources
     *  (see mesos.proto for a description of Filters). Available resources are aggregated
     *  when multiple offers are provided. Note that all offers must belong to the same slave.
     */
    void acceptOffers(List<OfferID> offerIds, List<Operation> operations, Filters filters);

    /**
     *  Declines the given offers in their entirety and applies the specified filters on the
     *  resources (see mesos.proto for a description of Filters). Note that this can be done
     *  at any time, it is not necessary to do this within the Scheduler::resourceOffers callback.
     */
    void declineOffers(List<OfferID> offerIds, Filters filters);

    /**
     *  Removes all filters previously set by the framework (via launchTasks()).  This
     *  enables the framework to receive offers from those filtered slaves.
     */
    void reviveOffers();

    /**
     *  Inform Mesos master to stop sending offers to the framework. The scheduler should
     *  call reviveOffers() to resume getting offers.
     */
    void suppressOffers();

    /**
     * Acknowledges the status update. This should only be called once the status update is
     * processed durably by the scheduler. Not that explicit acknowledgements must be
     * requested via the constructor argument, otherwise a call to this method will cause
     * the driver to crash.
     */
    void acknowledgeStatusUpdate(TaskStatus status);

    /**
     * Sends a message from the framework to one of its executors. These messages are best
     * effort; do not expect a framework message to be retransmitted in any reliable fashion.
     */
    void sendFrameworkMessage(ExecutorID executorId, AgentID agentId, ByteString data);

    /**
     * Allows the framework to query the status for non-terminal tasks. This causes the master
     * to send back the latest task status for each task in 'statuses', if possible. Tasks
     * that are no longer known will result in a TASK_LOST update. If statuses is empty, then
     * the master will send the latest status for each task currently known.
     */
    void reconcileTasks(List<TaskInfo> tasks);
}
