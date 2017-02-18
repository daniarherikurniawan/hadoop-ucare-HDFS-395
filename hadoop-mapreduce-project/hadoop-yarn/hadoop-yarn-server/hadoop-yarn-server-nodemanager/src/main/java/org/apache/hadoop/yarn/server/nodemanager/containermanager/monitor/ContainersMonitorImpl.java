/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NMConfig;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.ProcfsBasedProcessTree;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;

public class ContainersMonitorImpl extends AbstractService implements
    ContainersMonitor {

  final static Log LOG = LogFactory
      .getLog(ContainersMonitorImpl.class);

  private final static String MONITORING_INTERVAL_CONFIG_KEY =
      NMConfig.NM_PREFIX + "containers-monitor.monitoring-interval";
  public static final String RESOURCE_CALCULATOR_PLUGIN_CONFIG_KEY =
      NMConfig.NM_PREFIX + "containers-monitor.resourcecalculatorplugin";
  public static final String NM_RESERVED_PHYSICALMEMORY_MB =
      NMConfig.NM_PREFIX + "reserved-physical-memory.mb";

  private final static int MONITORING_INTERVAL_DEFAULT = 3000;
  private long monitoringInterval;
  private MonitoringThread monitoringThread;

  final List<ContainerId> containersToBeRemoved;
  final Map<ContainerId, ProcessTreeInfo> containersToBeAdded;
  Map<ContainerId, ProcessTreeInfo> trackingContainers =
      new HashMap<ContainerId, ProcessTreeInfo>();

  final ContainerExecutor containerExecutor;
  private final Dispatcher eventDispatcher;
  private final Context context;
  private ResourceCalculatorPlugin resourceCalculatorPlugin;

  private long maxVmemAllottedForContainers = DISABLED_MEMORY_LIMIT;
  private long maxPmemAllottedForContainers = DISABLED_MEMORY_LIMIT;

  /**
   * A value which if set for memory related configuration options, indicates
   * that the options are turned off.
   */
  public static final long DISABLED_MEMORY_LIMIT = -1L;

  private static final String MEMORY_USAGE_STRING =
      "Memory usage of ProcessTree %s for container-id %s : Virtual %d bytes, "
          +
          "limit : %d bytes; Physical %d bytes, limit %d bytes";

  public ContainersMonitorImpl(ContainerExecutor exec,
      AsyncDispatcher dispatcher, Context context) {
    super("containers-monitor");

    this.containerExecutor = exec;
    this.eventDispatcher = dispatcher;
    this.context = context;

    this.containersToBeAdded = new HashMap<ContainerId, ProcessTreeInfo>();
    this.containersToBeRemoved = new ArrayList<ContainerId>();
    this.monitoringThread = new MonitoringThread();
  }

  @Override
  public synchronized void init(Configuration conf) {
    this.monitoringInterval =
        conf.getLong(MONITORING_INTERVAL_CONFIG_KEY,
            MONITORING_INTERVAL_DEFAULT);

    Class<? extends ResourceCalculatorPlugin> clazz =
        conf.getClass(RESOURCE_CALCULATOR_PLUGIN_CONFIG_KEY, null,
            ResourceCalculatorPlugin.class);
    this.resourceCalculatorPlugin =
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(clazz, conf);
    LOG.info(" Using ResourceCalculatorPlugin : "
        + this.resourceCalculatorPlugin);

    long totalPhysicalMemoryOnNM = DISABLED_MEMORY_LIMIT;
    if (this.resourceCalculatorPlugin != null) {
      totalPhysicalMemoryOnNM =
          this.resourceCalculatorPlugin.getPhysicalMemorySize();
      if (totalPhysicalMemoryOnNM <= 0) {
        LOG.warn("NodeManager's totalPmem could not be calculated. "
            + "Setting it to " + DISABLED_MEMORY_LIMIT);
        totalPhysicalMemoryOnNM = DISABLED_MEMORY_LIMIT;
      }
    }

    // ///////// Virtual memory configuration //////
    this.maxVmemAllottedForContainers =
        conf.getLong(NMConfig.NM_VMEM_GB, NMConfig.DEFAULT_NM_VMEM_GB);
    this.maxVmemAllottedForContainers =
        this.maxVmemAllottedForContainers * 1024 * 1024 * 1024L; //Normalize

    if (this.maxVmemAllottedForContainers > totalPhysicalMemoryOnNM) {
      LOG.info("totalMemoryAllottedForContainers > totalPhysicalMemoryOnNM."
          + " Thrashing might happen.");
    }

    // ///////// Physical memory configuration //////
    long reservedPmemOnNM =
        conf.getLong(NM_RESERVED_PHYSICALMEMORY_MB, DISABLED_MEMORY_LIMIT);
    reservedPmemOnNM =
        reservedPmemOnNM == DISABLED_MEMORY_LIMIT
            ? DISABLED_MEMORY_LIMIT
            : reservedPmemOnNM * 1024 * 1024; // normalize to bytes

    if (reservedPmemOnNM == DISABLED_MEMORY_LIMIT
        || totalPhysicalMemoryOnNM == DISABLED_MEMORY_LIMIT) {
      this.maxPmemAllottedForContainers = DISABLED_MEMORY_LIMIT;
    } else {
      this.maxPmemAllottedForContainers =
          totalPhysicalMemoryOnNM - reservedPmemOnNM;
    }

    super.init(conf);
  }

  /**
   * Is the total physical memory check enabled?
   * 
   * @return true if total physical memory check is enabled.
   */
  boolean isPhysicalMemoryCheckEnabled() {
    return !(this.maxPmemAllottedForContainers == DISABLED_MEMORY_LIMIT);
  }

  /**
   * Is the total virtual memory check enabled?
   * 
   * @return true if total virtual memory check is enabled.
   */
  boolean isVirtualMemoryCheckEnabled() {
    return !(this.maxVmemAllottedForContainers == DISABLED_MEMORY_LIMIT);
  }

  private boolean isEnabled() {
    if (!ProcfsBasedProcessTree.isAvailable()) {
      LOG.info("ProcessTree implementation is missing on this system. "
          + this.getClass().getName() + " is disabled.");
      return false;
    }

    if (!(isPhysicalMemoryCheckEnabled() || isVirtualMemoryCheckEnabled())) {
      LOG.info("Neither virutal-memory nor physical-memory monitoring is " +
          "needed. Not running the monitor-thread");
      return false;
    }

    return true;
  }

  @Override
  public synchronized void start() {
    if (this.isEnabled()) {
      this.monitoringThread.start();
    }
    super.start();
  }

  @Override
  public synchronized void stop() {
    if (this.isEnabled()) {
      this.monitoringThread.interrupt();
      try {
        this.monitoringThread.join();
      } catch (InterruptedException e) {
        ;
      }
    }
    super.stop();
  }

  private static class ProcessTreeInfo {
    private ContainerId containerId;
    private String pid;
    private ProcfsBasedProcessTree pTree;
    private long vmemLimit;
    private long pmemLimit;

    public ProcessTreeInfo(ContainerId containerId, String pid,
        ProcfsBasedProcessTree pTree, long vmemLimit, long pmemLimit) {
      this.containerId = containerId;
      this.pid = pid;
      this.pTree = pTree;
      this.vmemLimit = vmemLimit;
      this.pmemLimit = pmemLimit;
    }

    public ContainerId getContainerId() {
      return this.containerId;
    }

    public String getPID() {
      return this.pid;
    }

    public void setPid(String pid) {
      this.pid = pid;
    }

    public ProcfsBasedProcessTree getProcessTree() {
      return this.pTree;
    }

    public void setProcessTree(ProcfsBasedProcessTree pTree) {
      this.pTree = pTree;
    }

    public long getVmemLimit() {
      return this.vmemLimit;
    }

    /**
     * @return Physical memory limit for the process tree in bytes
     */
    public long getPmemLimit() {
      return this.pmemLimit;
    }
  }


  /**
   * Check whether a container's process tree's current memory usage is over
   * limit.
   * 
   * When a java process exec's a program, it could momentarily account for
   * double the size of it's memory, because the JVM does a fork()+exec()
   * which at fork time creates a copy of the parent's memory. If the
   * monitoring thread detects the memory used by the container tree at the
   * same instance, it could assume it is over limit and kill the tree, for no
   * fault of the process itself.
   * 
   * We counter this problem by employing a heuristic check: - if a process
   * tree exceeds the memory limit by more than twice, it is killed
   * immediately - if a process tree has processes older than the monitoring
   * interval exceeding the memory limit by even 1 time, it is killed. Else it
   * is given the benefit of doubt to lie around for one more iteration.
   * 
   * @param containerId
   *          Container Id for the container tree
   * @param currentMemUsage
   *          Memory usage of a container tree
   * @param curMemUsageOfAgedProcesses
   *          Memory usage of processes older than an iteration in a container
   *          tree
   * @param vmemLimit
   *          The limit specified for the container
   * @return true if the memory usage is more than twice the specified limit,
   *         or if processes in the tree, older than this thread's monitoring
   *         interval, exceed the memory limit. False, otherwise.
   */
  boolean isProcessTreeOverLimit(String containerId,
                                  long currentMemUsage,
                                  long curMemUsageOfAgedProcesses,
                                  long vmemLimit) {
    boolean isOverLimit = false;

    if (currentMemUsage > (2 * vmemLimit)) {
      LOG.warn("Process tree for container: " + containerId
          + " running over twice " + "the configured limit. Limit=" + vmemLimit
          + ", current usage = " + currentMemUsage);
      isOverLimit = true;
    } else if (curMemUsageOfAgedProcesses > vmemLimit) {
      LOG.warn("Process tree for container: " + containerId
          + " has processes older than 1 "
          + "iteration running over the configured limit. Limit=" + vmemLimit
          + ", current usage = " + curMemUsageOfAgedProcesses);
      isOverLimit = true;
    }

    return isOverLimit;
  }

  // method provided just for easy testing purposes
  boolean isProcessTreeOverLimit(ProcfsBasedProcessTree pTree,
      String containerId, long limit) {
    long currentMemUsage = pTree.getCumulativeVmem();
    // as processes begin with an age 1, we want to see if there are processes
    // more than 1 iteration old.
    long curMemUsageOfAgedProcesses = pTree.getCumulativeVmem(1);
    return isProcessTreeOverLimit(containerId, currentMemUsage,
                                  curMemUsageOfAgedProcesses, limit);
  }

  private class MonitoringThread extends Thread {
    public MonitoringThread() {

    }

    @Override
    public void run() {

      while (true) {

        // Print the processTrees for debugging.
        if (LOG.isDebugEnabled()) {
          StringBuilder tmp = new StringBuilder("[ ");
          for (ProcessTreeInfo p : trackingContainers.values()) {
            tmp.append(p.getPID());
            tmp.append(" ");
          }
          LOG.debug("Current ProcessTree list : "
              + tmp.substring(0, tmp.length()) + "]");
        }

        // Add new containers
        synchronized (containersToBeAdded) {
          for (Entry<ContainerId, ProcessTreeInfo> entry : containersToBeAdded
              .entrySet()) {
            ContainerId containerId = entry.getKey();
            ProcessTreeInfo processTreeInfo = entry.getValue();
            LOG.info("Starting resource-monitoring for " + containerId);
            trackingContainers.put(containerId, processTreeInfo);
          }
          containersToBeAdded.clear();
        }

        // Remove finished containers
        synchronized (containersToBeRemoved) {
          for (ContainerId containerId : containersToBeRemoved) {
            trackingContainers.remove(containerId);
            LOG.info("Stopping resource-monitoring for " + containerId);
          }
          containersToBeRemoved.clear();
        }

        // Now do the monitoring for the trackingContainers
        // Check memory usage and kill any overflowing containers
        long vmemStillInUsage = 0;
        long pmemStillInUsage = 0;
        for (Iterator<Map.Entry<ContainerId, ProcessTreeInfo>> it =
            trackingContainers.entrySet().iterator(); it.hasNext();) {

          Map.Entry<ContainerId, ProcessTreeInfo> entry = it.next();
          ContainerId containerId = entry.getKey();
          ProcessTreeInfo ptInfo = entry.getValue();
          try {
            String pId = ptInfo.getPID();

            // Initialize any uninitialized processTrees
            if (pId == null) {
              // get pid from ContainerId
              pId = containerExecutor.getProcessId(ptInfo.getContainerId());
              if (pId != null) {
                // pId will be null, either if the container is not spawned yet
                // or if the container's pid is removed from ContainerExecutor
                LOG.debug("Tracking ProcessTree " + pId
                    + " for the first time");

                ProcfsBasedProcessTree pt =
                    new ProcfsBasedProcessTree(pId,
                        ContainerExecutor.isSetsidAvailable);
                ptInfo.setPid(pId);
                ptInfo.setProcessTree(pt);
              }
            }
            // End of initializing any uninitialized processTrees

            if (pId == null) {
              continue; // processTree cannot be tracked
            }

            LOG.debug("Constructing ProcessTree for : PID = " + pId
                + " ContainerId = " + containerId);
            ProcfsBasedProcessTree pTree = ptInfo.getProcessTree();
            pTree = pTree.getProcessTree(); // get the updated process-tree
            ptInfo.setProcessTree(pTree); // update ptInfo with proces-tree of
                                          // updated state
            long currentVmemUsage = pTree.getCumulativeVmem();
            long currentPmemUsage = pTree.getCumulativeRssmem();
            // as processes begin with an age 1, we want to see if there
            // are processes more than 1 iteration old.
            long curMemUsageOfAgedProcesses = pTree.getCumulativeVmem(1);
            long curRssMemUsageOfAgedProcesses = pTree.getCumulativeRssmem(1);
            long vmemLimit = ptInfo.getVmemLimit();
            long pmemLimit = ptInfo.getPmemLimit();
            LOG.info(String.format(MEMORY_USAGE_STRING, pId,
                containerId.toString(), currentVmemUsage, vmemLimit,
                currentPmemUsage, pmemLimit));

            boolean isMemoryOverLimit = false;
            String msg = "";
            if (isVirtualMemoryCheckEnabled()
                && isProcessTreeOverLimit(containerId.toString(),
                    currentVmemUsage, curMemUsageOfAgedProcesses, vmemLimit)) {
              // Container (the root process) is still alive and overflowing
              // memory.
              // Dump the process-tree and then clean it up.
              msg =
                  "Container [pid="
                      + pId
                      + ",containerID="
                      + containerId
                      + "] is running beyond memory-limits. Current usage : "
                      + currentVmemUsage
                      + "bytes. Limit : "
                      + vmemLimit
                      + "bytes. Killing container. "
                      + "\nDump of the process-tree for " + containerId
                      + " : \n" + pTree.getProcessTreeDump();
              isMemoryOverLimit = true;
            } else if (isPhysicalMemoryCheckEnabled()
                && isProcessTreeOverLimit(containerId.toString(),
                    currentPmemUsage, curRssMemUsageOfAgedProcesses,
                    pmemLimit)) {
              // Container (the root process) is still alive and overflowing
              // memory.
              // Dump the process-tree and then clean it up.
              msg =
                  "Container [pid="
                      + pId
                      + ",tipID="
                      + containerId
                      + "] is running beyond physical memory-limits."
                      + " Current usage : "
                      + currentPmemUsage
                      + "bytes. Limit : "
                      + pmemLimit
                      + "bytes. Killing container. \nDump of the process-tree for "
                      + containerId + " : \n" + pTree.getProcessTreeDump();
              isMemoryOverLimit = true;
            }

            if (isMemoryOverLimit) {
              // Virtual or physical memory over limit. Fail the container and
              // remove
              // the corresponding process tree
              LOG.warn(msg);
              // warn if not a leader
              if (!pTree.checkPidPgrpidForMatch()) {
                LOG.error("Killed container process with PID " + pId
                    + " but it is not a process group leader.");
              }
              // kill the container
              eventDispatcher.getEventHandler().handle(
                  new ContainerKillEvent(containerId, msg));
              it.remove();
              LOG.info("Removed ProcessTree with root " + pId);
            } else {
              // Accounting the total memory in usage for all containers that
              // are still
              // alive and within limits.
              vmemStillInUsage += currentVmemUsage;
              pmemStillInUsage += currentPmemUsage;
            }
          } catch (Exception e) {
            // Log the exception and proceed to the next container.
            LOG.warn("Uncaught exception in ContainerMemoryManager "
                + "while managing memory of " + containerId, e);
          }
        }

        try {
          Thread.sleep(monitoringInterval);
        } catch (InterruptedException e) {
          LOG.warn(ContainersMonitorImpl.class.getName()
              + " is interrupted. Exiting.");
          break;
        }
      }
    }
  }

  @Override
  public long getVmemAllocatedForContainers() {
    return this.maxVmemAllottedForContainers;
  }

  @Override
  public long getPmemAllocatedForContainers() {
    return this.maxPmemAllottedForContainers;
  }

  @Override
  public void handle(ContainersMonitorEvent monitoringEvent) {

    if (!isEnabled()) {
      return;
    }

    ContainerId containerId = monitoringEvent.getContainerId();
    switch (monitoringEvent.getType()) {
    case START_MONITORING_CONTAINER:
      ContainerStartMonitoringEvent startEvent =
          (ContainerStartMonitoringEvent) monitoringEvent;
      synchronized (this.containersToBeAdded) {
        ProcessTreeInfo processTreeInfo =
            new ProcessTreeInfo(containerId, null, null,
                startEvent.getVmemLimit(), startEvent.getPmemLimit());
        this.containersToBeAdded.put(containerId, processTreeInfo);
      }
      break;
    case STOP_MONITORING_CONTAINER:
      synchronized (this.containersToBeRemoved) {
        this.containersToBeRemoved.add(containerId);
      }
      break;
    default:
      // TODO: Wrong event.
    }
  }
}
