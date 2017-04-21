package com.duedil.mesos.java.executor;

import org.apache.mesos.v1.Protos.AgentInfo;
import org.apache.mesos.v1.Protos.ExecutorInfo;
import org.apache.mesos.v1.Protos.FrameworkInfo;
import org.apache.mesos.v1.Protos.TaskID;
import org.apache.mesos.v1.Protos.TaskInfo;


/**
 * Base class for Mesos executors. Users' executors should extend this class to get default
 * implementations of methods they don't override.
 */
public interface Executor {

    /**
     * Invoked once the executor driver has been able to successfully connect with Mesos.
     * In particular, a scheduler can pass some data to its executors through the
     * FrameworkInfo.ExecutorInfo's data field.
     */
    void registered(ExecutorDriver driver, ExecutorInfo executorInfo, FrameworkInfo frameworkInfo, AgentInfo agentInfo);

    /**
     * Invoked when the executor re-registers with a restarted slave.
     */
    void reregistered(ExecutorDriver driver, AgentInfo agentInfo);

    /**
     * Invoked when the executor becomes "disconnected" from the slave (e.g., the slave is
     * being restarted due to an upgrade).
     */
    void disconnected(ExecutorDriver driver);

    /**
     * Invoked when a task has been launched on this executor (initiated via
     * Scheduler.launchTasks).  Note that this task can be realized with a thread, a process,
     * or some simple computation, however, no other callbacks will be invoked on this executor
     * until this callback has returned.
     */
    void launchTask(ExecutorDriver driver, TaskInfo task);

    /**
     * Invoked when a task running within this executor has been killed (via
     * SchedulerDriver.killTask).  Note that no status update will be sent on behalf of the
     * executor, the executor is responsible for creating a new TaskStatus (i.e., with
     * TASK_KILLED) and invoking ExecutorDriver's sendStatusUpdate.
     */
    void killTask(ExecutorDriver driver, TaskID taskId);

    /**
     * Invoked when a framework message has arrived for this executor.  These messages are
     * best effort; do not expect a framework message to be retransmitted in any reliable fashion.
     */
    void frameworkMessage(ExecutorDriver driver, byte[] message);

    /**
     * Invoked when the executor should terminate all of its currently running tasks.  Note
     * that after Mesos has determined that an executor has terminated any tasks that the
     * executor did not send terminal status updates for (e.g., TASK_KILLED, TASK_FINISHED,
     * TASK_FAILED, etc) a TASK_LOST status update will be created.
     */
    void shutdown(ExecutorDriver driver);

    /**
     * Invoked when a fatal error has occurred with the executor and/or executor driver.
     * The driver will be aborted BEFORE invoking this callback.
     */
    void error(ExecutorDriver driver, String message);
}
