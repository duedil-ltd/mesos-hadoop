package org.apache.hadoop.mapred;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

import org.apache.commons.httpclient.HttpHost;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.StringUtils;
import org.apache.mesos.v1.Protos.TaskID;
import org.apache.mesos.v1.Protos.TaskInfo;
import org.apache.mesos.v1.Protos.CommandInfo;
import org.apache.mesos.v1.Protos.Environment;
import org.apache.mesos.v1.Protos.Environment.Variable;
import org.apache.mesos.v1.Protos.ExecutorInfo;
import org.apache.mesos.v1.Protos.ExecutorID;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.Offer;
import org.apache.mesos.v1.Protos.Offer.Operation;
import org.apache.mesos.v1.Protos.Offer.Operation.Launch;
import org.apache.mesos.v1.Protos.OfferID;
import org.apache.mesos.v1.Protos.Resource;
import org.apache.mesos.v1.Protos.Value.Range;
import org.apache.mesos.v1.Protos.Value.Ranges;
import org.apache.mesos.v1.Protos.Value.Scalar;
import org.apache.mesos.v1.scheduler.Protos.Call;
import org.apache.mesos.v1.scheduler.Protos.Call.Accept;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.mesosphere.mesos.rx.java.protobuf.SchedulerCalls.decline;
import static org.apache.hadoop.util.StringUtils.join;
import static org.apache.mesos.v1.Protos.Offer.Operation.Type.LAUNCH;
import static org.apache.mesos.v1.Protos.Value.Type.RANGES;
import static org.apache.mesos.v1.Protos.Value.Type.SCALAR;
import static org.apache.mesos.v1.scheduler.Protos.Call.Type.ACCEPT;

public class NewResourcePolicy {
  public static final Log LOG = LogFactory.getLog(ResourcePolicy.class);
  public volatile MesosScheduler scheduler;
  public int neededMapSlots;
  public int neededReduceSlots;
  public long slots, mapSlots, reduceSlots;
  private int mapSlotsMax, reduceSlotsMax;
  private double slotCpus;
  private double slotDisk;
  private int slotMem;
  private long slotJVMHeap;
  private int tasktrackerMem;
  private long tasktrackerJVMHeap;
  // Minimum resource requirements for the container (TaskTracker + map/red
  // tasks).
  private double containerCpus;
  private double containerMem;
  private double containerDisk;

  public NewResourcePolicy(MesosScheduler scheduler) {
    this.scheduler = scheduler;

    mapSlotsMax = scheduler.conf.getInt("mapred.tasktracker.map.tasks.maximum",
        MesosScheduler.MAP_SLOTS_DEFAULT);
    reduceSlotsMax =
        scheduler.conf.getInt("mapred.tasktracker.reduce.tasks.maximum",
            MesosScheduler.REDUCE_SLOTS_DEFAULT);

    slotCpus = scheduler.conf.getFloat("mapred.mesos.slot.cpus",
        (float) MesosScheduler.SLOT_CPUS_DEFAULT);
    slotDisk = scheduler.conf.getInt("mapred.mesos.slot.disk",
        MesosScheduler.SLOT_DISK_DEFAULT);

    slotMem = scheduler.conf.getInt("mapred.mesos.slot.mem",
        MesosScheduler.SLOT_JVM_HEAP_DEFAULT);
    slotJVMHeap = Math.round((double) slotMem /
        (MesosScheduler.JVM_MEM_OVERHEAD_PERCENT_DEFAULT + 1));

    tasktrackerMem = scheduler.conf.getInt("mapred.mesos.tasktracker.mem",
        MesosScheduler.TASKTRACKER_MEM_DEFAULT);
    tasktrackerJVMHeap = Math.round((double) tasktrackerMem /
        (MesosScheduler.JVM_MEM_OVERHEAD_PERCENT_DEFAULT + 1));

    containerCpus = scheduler.conf.getFloat("mapred.mesos.tasktracker.cpus",
        (float) MesosScheduler.TASKTRACKER_CPUS_DEFAULT);
    containerDisk = scheduler.conf.getInt("mapred.mesos.tasktracker.disk",
        MesosScheduler.TASKTRACKER_DISK_DEFAULT);

    containerMem = tasktrackerMem;
  }

  public void computeNeededSlots(List<JobInProgress> jobsInProgress,
                                 Collection<TaskTrackerStatus> taskTrackers) {
    // Compute the number of pending maps and reduces.
    int pendingMaps = 0;
    int pendingReduces = 0;
    int runningMaps = 0;
    int runningReduces = 0;

    for (JobInProgress progress : jobsInProgress) {
      // JobStatus.pendingMaps/Reduces may return the wrong value on
      // occasion.  This seems to be safer.
      pendingMaps += scheduler.getPendingTasks(progress.getTasks(TaskType.MAP));
      pendingReduces += scheduler.getPendingTasks(progress.getTasks(TaskType.REDUCE));
      runningMaps += progress.runningMaps();
      runningReduces += progress.runningReduces();

      // If the task is waiting to launch the cleanup task, let us make sure we have
      // capacity to run the task.
      if (!progress.isCleanupLaunched()) {
        pendingMaps += scheduler.getPendingTasks(progress.getTasks(TaskType.JOB_CLEANUP));
      }
    }

    // Mark active (heartbeated) TaskTrackers and compute idle slots.
    int idleMapSlots = 0;
    int idleReduceSlots = 0;
    int unhealthyTrackers = 0;

    for (TaskTrackerStatus status : taskTrackers) {
      if (!status.getHealthStatus().isNodeHealthy()) {
        // Skip this node if it's unhealthy.
        ++unhealthyTrackers;
        continue;
      }

      HttpHost host = new HttpHost(status.getHost(), status.getHttpPort());
      if (scheduler.mesosTrackers.containsKey(host)) {
        scheduler.mesosTrackers.get(host).active = true;
        idleMapSlots += status.getAvailableMapSlots();
        idleReduceSlots += status.getAvailableReduceSlots();
      }
    }

    // Consider the TaskTrackers that have yet to become active as being idle,
    // otherwise we will launch excessive TaskTrackers.
    int inactiveMapSlots = 0;
    int inactiveReduceSlots = 0;
    for (MesosTracker tracker : scheduler.mesosTrackers.values()) {
      if (!tracker.active) {
        inactiveMapSlots += tracker.mapSlots;
        inactiveReduceSlots += tracker.reduceSlots;
      }
    }

    // To ensure Hadoop jobs begin promptly, we can specify a minimum number
    // of 'hot slots' to be available for use.  This addresses the
    // TaskTracker spin up delay that exists with Hadoop on Mesos.  This can
    // be a nuisance with lower latency applications, such as ad-hoc Hive
    // queries.
    int minimumMapSlots = scheduler.conf.getInt("mapred.mesos.total.map.slots.minimum", 0);
    int minimumReduceSlots =
        scheduler.conf.getInt("mapred.mesos.total.reduce.slots.minimum", 0);

    // Compute how many slots we need to allocate.
    neededMapSlots = Math.max(
        minimumMapSlots - (idleMapSlots + inactiveMapSlots),
        pendingMaps - (idleMapSlots + inactiveMapSlots));
    neededReduceSlots = Math.max(
        minimumReduceSlots - (idleReduceSlots + inactiveReduceSlots),
        pendingReduces - (idleReduceSlots + inactiveReduceSlots));

    LOG.info(join("\n", Arrays.asList(
        "JobTracker Status",
        "      Pending Map Tasks: " + pendingMaps,
        "   Pending Reduce Tasks: " + pendingReduces,
        "      Running Map Tasks: " + runningMaps,
        "   Running Reduce Tasks: " + runningReduces,
        "         Idle Map Slots: " + idleMapSlots,
        "      Idle Reduce Slots: " + idleReduceSlots,
        "     Inactive Map Slots: " + inactiveMapSlots
            + " (launched but no hearbeat yet)",
        "  Inactive Reduce Slots: " + inactiveReduceSlots
            + " (launched but no hearbeat yet)",
        "       Needed Map Slots: " + neededMapSlots,
        "    Needed Reduce Slots: " + neededReduceSlots,
        "     Unhealthy Trackers: " + unhealthyTrackers)));

    if (scheduler.stateFile != null) {
      // Update state file
      synchronized (this) {
        Set<String> hosts = new HashSet<String>();
        for (MesosTracker tracker : scheduler.mesosTrackers.values()) {
          hosts.add(tracker.host.getHostName());
        }
        try {
          File tmp = new File(scheduler.stateFile.getAbsoluteFile() + ".tmp");
          FileWriter fstream = new FileWriter(tmp);
          fstream.write(join("\n", Arrays.asList(
              "time=" + System.currentTimeMillis(),
              "pendingMaps=" + pendingMaps,
              "pendingReduces=" + pendingReduces,
              "runningMaps=" + runningMaps,
              "runningReduces=" + runningReduces,
              "idleMapSlots=" + idleMapSlots,
              "idleReduceSlots=" + idleReduceSlots,
              "inactiveMapSlots=" + inactiveMapSlots,
              "inactiveReduceSlots=" + inactiveReduceSlots,
              "neededMapSlots=" + neededMapSlots,
              "neededReduceSlots=" + neededReduceSlots,
              "unhealthyTrackers=" + unhealthyTrackers,
              "hosts=" + join(",", hosts),
              "")));
          fstream.close();
          tmp.renameTo(scheduler.stateFile);
        } catch (Exception e) {
          LOG.error("Can't write state file: " + e.getMessage());
        }
      }
    }
  }

  // This method computes the number of slots to launch for this offer, and
  // returns true if the offer is sufficient.
  // Must be overridden.
  public boolean computeSlots() {
    return false;
  }

  public List<Call> resourceOffers(FrameworkID frameworkId, List<Offer> offers) {
    final HttpHost jobTrackerAddress =
            new HttpHost(scheduler.jobTracker.getHostname(), scheduler.jobTracker.getTrackerPort());

    final Collection<TaskTrackerStatus> taskTrackers = scheduler.jobTracker.taskTrackers();

    final List<JobInProgress> jobsInProgress = new ArrayList<JobInProgress>();
    for (JobStatus status : scheduler.jobTracker.jobsToComplete()) {
      jobsInProgress.add(scheduler.jobTracker.getJob(status.getJobID()));
    }

    synchronized (this) {
      computeNeededSlots(jobsInProgress, taskTrackers);

      // Launch TaskTrackers to satisfy the slot requirements.
      List<Call> calls = offers.stream()
              .map(o -> processOffer(frameworkId, o))
              .collect(Collectors.toList());

      if (neededMapSlots <= 0 && neededReduceSlots <= 0) {
        LOG.info("Satisfied map and reduce slots needed.");
      } else {
        LOG.info("Unable to fully satisfy needed map/reduce slots: "
                + (neededMapSlots > 0 ? neededMapSlots + " map slots " : "")
                + (neededReduceSlots > 0 ? neededReduceSlots + " reduce slots " : "")
                + "remaining");
      }
      // TODO is returning here going to break synchronisation? Have the calls actually been made at this point?
      return calls;
    }
  }

  private Call processOffer(FrameworkID frameworkId, Offer offer) {
    if (neededMapSlots <= 0 && neededReduceSlots <= 0) {
      return decline(frameworkId, Arrays.asList(offer.getId()));
    }

    // Ensure these values aren't < 0.
    neededMapSlots = Math.max(0, neededMapSlots);
    neededReduceSlots = Math.max(0, neededReduceSlots);

    double cpus = -1.0;
    double mem = -1.0;
    double disk = -1.0;
    Set<Integer> ports = new HashSet<>();
    String cpuRole = "*";
    String memRole = cpuRole;
    String diskRole = cpuRole;
    String portsRole = cpuRole;

    // Pull out the cpus, memory, disk, and 2 ports from the offer.
    for (Resource resource : offer.getResourcesList()) {
      if (resource.getName().equals("cpus")
              && resource.getType() == SCALAR) {
        cpus = resource.getScalar().getValue();
        cpuRole = resource.getRole();
      } else if (resource.getName().equals("mem")
              && resource.getType() == SCALAR) {
        mem = resource.getScalar().getValue();
        memRole = resource.getRole();
      } else if (resource.getName().equals("disk")
              && resource.getType() == SCALAR) {
        disk = resource.getScalar().getValue();
        diskRole = resource.getRole();
      } else if (resource.getName().equals("ports")
              && resource.getType() == RANGES) {
        portsRole = resource.getRole();
        for (Range range : resource.getRanges().getRangeList()) {
          Integer begin = (int) range.getBegin();
          Integer end = (int) range.getEnd();
          if (end < begin) {
            LOG.warn("Ignoring invalid port range: begin=" + begin + " end=" + end);
            continue;
          }
          while (begin <= end && ports.size() < 2) {
            int port = begin + (int)(Math.random() * ((end - begin) + 1));
            ports.add(port);
            begin += 1;
          }
        }
      }
    }

    // Verify the resource roles are what we need
    if (scheduler.conf.getBoolean("mapred.mesos.role.strict", false)) {
      String expectedRole = scheduler.conf.get("mapred.mesos.role", "*");
      if (!cpuRole.equals(expectedRole) ||
              !memRole.equals(expectedRole) ||
              !diskRole.equals(expectedRole) ||
              !portsRole.equals(expectedRole)) {
        LOG.info("Declining offer with invalid role " + expectedRole);

        decline(frameworkId, Arrays.asList(offer.getId()));
      }
    }

    final boolean sufficient = computeSlots();

    double taskCpus = (mapSlots + reduceSlots) * slotCpus + containerCpus;
    double taskMem = (mapSlots + reduceSlots) * slotMem + containerMem;
    double taskDisk = (mapSlots + reduceSlots) * slotDisk + containerDisk;

    if (!sufficient || ports.size() < 2) {
      LOG.info(join("\n", Arrays.asList(
              "Declining offer with insufficient resources for a TaskTracker: ",
              "  cpus: offered " + cpus + " needed at least " + taskCpus,
              "  mem : offered " + mem + " needed at least " + taskMem,
              "  disk: offered " + disk + " needed at least " + taskDisk,
              "  ports: " + (ports.size() < 2
                      ? " less than 2 offered"
                      : " at least 2 (sufficient)"))));

      decline(frameworkId, Arrays.asList(offer.getId()));
    }

    Iterator<Integer> portIter = ports.iterator();
    HttpHost httpAddress = new HttpHost(offer.getHostname(), portIter.next());
    HttpHost reportAddress = new HttpHost(offer.getHostname(), portIter.next());

    // Check that this tracker is not already launched.  This problem was
    // observed on a few occasions, but not reliably.  The main symptom was
    // that entries in `mesosTrackers` were being lost, and task trackers
    // would be 'lost' mysteriously (probably because the ports were in
    // use).  This problem has since gone away with a rewrite of the port
    // selection code, but the check + logging is left here.
    // TODO(brenden): Diagnose this to determine root cause.
    if (scheduler.mesosTrackers.containsKey(httpAddress)) {
      LOG.info(join("\n", Arrays.asList(
              "Declining offer because host/port combination is in use: ",
              "  cpus: offered " + cpus + " needed " + taskCpus,
              "  mem : offered " + mem + " needed " + taskMem,
              "  disk: offered " + disk + " needed " + taskDisk,
              "  ports: " + ports)));

      decline(frameworkId, Arrays.asList(offer.getId()));
    }

    TaskID taskId = TaskID.newBuilder()
            .setValue("Task_Tracker_" + scheduler.launchedTrackers++).build();

    LOG.info("Launching task " + taskId.getValue() + " on "
            + httpAddress.toString() + " with mapSlots=" + mapSlots + " reduceSlots=" + reduceSlots);

    List<String> defaultJvmOpts = Arrays.asList(
            "-XX:+UseConcMarkSweepGC",
            "-XX:+CMSParallelRemarkEnabled",
            "-XX:+CMSClassUnloadingEnabled",
            "-XX:+UseParNewGC",
            "-XX:TargetSurvivorRatio=80",
            "-XX:+UseTLAB",
            "-XX:ParallelGCThreads=2",
            "-XX:+AggressiveOpts",
            "-XX:+UseCompressedOops",
            "-XX:+UseFastEmptyMethods",
            "-XX:+UseFastAccessorMethods",
            "-Xss512k",
            "-XX:+AlwaysPreTouch",
            "-XX:CMSInitiatingOccupancyFraction=80"
    );

    String jvmOpts = scheduler.conf.get("mapred.mesos.executor.jvm.opts");
    if (jvmOpts == null) {
      jvmOpts = StringUtils.join(" ", defaultJvmOpts);
    }

    // Set up the environment for running the TaskTracker.
    Environment.Builder envBuilder = Environment
            .newBuilder()
            .addVariables(
                    Environment.Variable.newBuilder()
                            .setName("HADOOP_OPTS")
                            .setValue(
                                    jvmOpts +
                                            " -Xmx" + tasktrackerJVMHeap + "m" +
                                            " -XX:NewSize=" + tasktrackerJVMHeap / 3 + "m -XX:MaxNewSize=" + (int)Math.floor
                                            (tasktrackerJVMHeap * 0.6) + "m"
                            ));

    // Set java specific environment, appropriately.
    Map<String, String> env = System.getenv();
    if (env.containsKey("JAVA_HOME")) {
      envBuilder.addVariables(Variable.newBuilder()
              .setName("JAVA_HOME")
              .setValue(env.get("JAVA_HOME")));
    }

    if (env.containsKey("JAVA_LIBRARY_PATH")) {
      envBuilder.addVariables(Variable.newBuilder()
              .setName("JAVA_LIBRARY_PATH")
              .setValue(env.get("JAVA_LIBRARY_PATH")));
    }

    // Command info differs when performing a local run.
    String master = scheduler.conf.get("mapred.mesos.master");

    if (master == null) {
      throw new RuntimeException(
              "Expecting configuration property 'mapred.mesos.master'");
    } else if (master == "local") {
      throw new RuntimeException(
              "Can not use 'local' for 'mapred.mesos.executor'");
    }

    String uri = scheduler.conf.get("mapred.mesos.executor.uri");
    String directory = scheduler.conf.get("mapred.mesos.executor.directory");
    boolean isUriSet = uri != null && !uri.equals("");
    boolean isDirectorySet = directory != null && !directory.equals("");

    if (!isUriSet && !isDirectorySet) {
      throw new RuntimeException(
              "Expecting configuration property 'mapred.mesos.executor'");
    } else if (isUriSet && isDirectorySet) {
      throw new RuntimeException(
              "Conflicting properties 'mapred.mesos.executor.uri' and 'mapred.mesos.executor.directory', only one can be set");
    } else if (!isDirectorySet) {
      LOG.info("URI: " + uri + ", name: " + new File(uri).getName());

      directory = new File(uri).getName().split("\\.")[0] + "*";
    } else if (!isUriSet) {
      LOG.info("mapred.mesos.executor.uri is not set, relying on configured 'mapred.mesos.executor.directory' for working Hadoop distribution");
    }

    String command = scheduler.conf.get("mapred.mesos.executor.command");
    if (command == null || command.equals("")) {
      command = "env ; ./bin/hadoop org.apache.hadoop.mapred.MesosExecutor";
    }

    CommandInfo.Builder commandInfo = CommandInfo.newBuilder();
    commandInfo
            .setEnvironment(envBuilder)
            .setValue(String.format("cd %s && %s", directory, command));
    if (uri != null) {
      commandInfo.addUris(CommandInfo.URI.newBuilder().setValue(uri));
    }

    // Populate old-style ContainerInfo if needed
    String containerImage = scheduler.conf.get("mapred.mesos.container.image");
    if (containerImage != null && !containerImage.equals("")) {
      commandInfo.setContainer(org.apache.mesos.hadoop.Utils.buildContainerInfo(scheduler.conf));
    }

    // Create a configuration from the current configuration and
    // override properties as appropriate for the TaskTracker.
    Configuration overrides = new Configuration(scheduler.conf);

    overrides.set("mapred.task.tracker.http.address",
            httpAddress.getHostName() + ':' + httpAddress.getPort());

    overrides.set("mapred.task.tracker.report.address",
            reportAddress.getHostName() + ':' + reportAddress.getPort());

    overrides.setLong("mapred.tasktracker.map.tasks.maximum", mapSlots);
    overrides.setLong("mapred.tasktracker.reduce.tasks.maximum", reduceSlots);

    // Build up the executor info
    ExecutorInfo.Builder executorBuilder = ExecutorInfo
            .newBuilder()
            .setExecutorId(ExecutorID.newBuilder().setValue(
                    "executor_" + taskId.getValue()))
            .setName("Hadoop TaskTracker")
            .setSource(taskId.getValue())
            .addResources(
                    Resource
                            .newBuilder()
                            .setName("cpus")
                            .setType(SCALAR)
                            .setRole(cpuRole)
                            .setScalar(Scalar.newBuilder().setValue(containerCpus)))
            .addResources(
                    Resource
                            .newBuilder()
                            .setName("mem")
                            .setType(SCALAR)
                            .setRole(memRole)
                            .setScalar(Scalar.newBuilder().setValue(containerMem)))
            .addResources(
                    Resource
                            .newBuilder()
                            .setName("disk")
                            .setType(SCALAR)
                            .setRole(diskRole)
                            .setScalar(Scalar.newBuilder().setValue(containerDisk)))
            .setCommand(commandInfo.build());

    // Add the docker container info if an image is specified
    String dockerImage = scheduler.conf.get("mapred.mesos.docker.image");
    if (dockerImage != null && !dockerImage.equals("")) {
      executorBuilder.setContainer(org.apache.mesos.hadoop.Utils.buildDockerContainerInfo(scheduler.conf));
    }

    ByteString taskData;

    try {
      taskData = org.apache.mesos.hadoop.Utils.confToBytes(overrides);
    } catch (IOException e) {
      LOG.error("Caught exception serializing configuration");

      // Skip this offer completely
      decline(frameworkId, Arrays.asList(offer.getId()));
    }

    // Create the TaskTracker TaskInfo
    TaskInfo trackerTaskInfo = TaskInfo
            .newBuilder()
            .setName(taskId.getValue())
            .setTaskId(taskId)
            .setAgentId(offer.getAgentId())
            .addResources(
                    Resource
                            .newBuilder()
                            .setName("ports")
                            .setType(RANGES)
                            .setRole(portsRole)
                            .setRanges(
                                    Ranges
                                            .newBuilder()
                                            .addRange(Range.newBuilder()
                                                    .setBegin(httpAddress.getPort())
                                                    .setEnd(httpAddress.getPort()))
                                            .addRange(Range.newBuilder()
                                                    .setBegin(reportAddress.getPort())
                                                    .setEnd(reportAddress.getPort()))))
            .addResources(
                    Resource
                            .newBuilder()
                            .setName("cpus")
                            .setType(SCALAR)
                            .setRole(cpuRole)
                            .setScalar(Scalar.newBuilder().setValue(taskCpus - containerCpus)))
            .addResources(
                    Resource
                            .newBuilder()
                            .setName("mem")
                            .setType(SCALAR)
                            .setRole(memRole)
                            .setScalar(Scalar.newBuilder().setValue(taskMem - containerCpus)))
            .setData(taskData)
            .setExecutor(executorBuilder.build())
            .build();

    // Add this tracker to Mesos tasks.
    scheduler.mesosTrackers.put(httpAddress, new MesosTracker(httpAddress, taskId, mapSlots, reduceSlots, scheduler));

    // Launch the task
    Call launch = Call.newBuilder()
            .setFrameworkId(frameworkId)
            .setType(ACCEPT)
            .setAccept(
                    Accept.newBuilder()
                            .addOfferIds(offer.getId())
                            .addOperations(
                              Operation.newBuilder()
                                      .setType(LAUNCH)
                                      .setLaunch(
                                              Launch.newBuilder()
                                                      .addAllTaskInfos(Arrays.asList(trackerTaskInfo))
                                      )
                            )
            )
            .build();

    neededMapSlots -= mapSlots;
    neededReduceSlots -= reduceSlots;

    return launch;
  }
}
