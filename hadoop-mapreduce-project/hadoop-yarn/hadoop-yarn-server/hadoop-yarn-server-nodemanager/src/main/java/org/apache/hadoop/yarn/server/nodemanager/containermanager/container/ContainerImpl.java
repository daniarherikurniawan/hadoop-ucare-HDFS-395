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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.event.LogAggregatorContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainerStartMonitoringEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainerStopMonitoringEvent;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class ContainerImpl implements Container {

  private final Lock readLock;
  private final Lock writeLock;
  private final Dispatcher dispatcher;
  private final Credentials credentials;
  private final NodeManagerMetrics metrics;
  private final ContainerLaunchContext launchContext;
  private String exitCode = "NA";
  private final StringBuilder diagnostics;

  private static final Log LOG = LogFactory.getLog(Container.class);
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private final Map<LocalResourceRequest,String> pendingResources =
    new HashMap<LocalResourceRequest,String>();
  private final Map<Path,String> localizedResources =
    new HashMap<Path,String>();

  public ContainerImpl(Dispatcher dispatcher,
      ContainerLaunchContext launchContext, Credentials creds,
      NodeManagerMetrics metrics) {
    this.dispatcher = dispatcher;
    this.launchContext = launchContext;
    this.diagnostics = new StringBuilder();
    this.credentials = creds;
    this.metrics = metrics;

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    stateMachine = stateMachineFactory.make(this);
  }

  private static final ContainerDoneTransition CONTAINER_DONE_TRANSITION =
    new ContainerDoneTransition();

  private static final ContainerDiagnosticsUpdateTransition UPDATE_DIAGNOSTICS_TRANSITION =
      new ContainerDiagnosticsUpdateTransition();

  // State Machine for each container.
  private static StateMachineFactory
           <ContainerImpl, ContainerState, ContainerEventType, ContainerEvent>
        stateMachineFactory =
      new StateMachineFactory<ContainerImpl, ContainerState, ContainerEventType, ContainerEvent>(ContainerState.NEW)
    // From NEW State
    .addTransition(ContainerState.NEW,
        EnumSet.of(ContainerState.LOCALIZING, ContainerState.LOCALIZED,
            ContainerState.LOCALIZATION_FAILED),
        ContainerEventType.INIT_CONTAINER, new RequestResourcesTransition())
    .addTransition(ContainerState.NEW, ContainerState.NEW,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.NEW, ContainerState.DONE,
        ContainerEventType.KILL_CONTAINER, CONTAINER_DONE_TRANSITION)

    // From LOCALIZING State
    .addTransition(ContainerState.LOCALIZING,
        EnumSet.of(ContainerState.LOCALIZING, ContainerState.LOCALIZED),
        ContainerEventType.RESOURCE_LOCALIZED, new LocalizedTransition())
    .addTransition(ContainerState.LOCALIZING,
        ContainerState.LOCALIZATION_FAILED,
        ContainerEventType.RESOURCE_FAILED,
        new ResourceFailedTransition())
    .addTransition(ContainerState.LOCALIZING, ContainerState.LOCALIZING,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.LOCALIZING, ContainerState.KILLING,
        ContainerEventType.KILL_CONTAINER,
        new KillDuringLocalizationTransition())

    // From LOCALIZATION_FAILED State
    .addTransition(ContainerState.LOCALIZATION_FAILED,
        ContainerState.DONE,
        ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
        CONTAINER_DONE_TRANSITION)
    .addTransition(ContainerState.LOCALIZATION_FAILED,
        ContainerState.LOCALIZATION_FAILED,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)

    // From LOCALIZED State
    .addTransition(ContainerState.LOCALIZED, ContainerState.RUNNING,
        ContainerEventType.CONTAINER_LAUNCHED, new LaunchTransition())
    .addTransition(ContainerState.LOCALIZED, ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
        new ExitedWithFailureTransition())
    .addTransition(ContainerState.LOCALIZED, ContainerState.LOCALIZED,
       ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
       UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.LOCALIZED, ContainerState.KILLING,
        ContainerEventType.KILL_CONTAINER, new KillTransition())

    // From RUNNING State
    .addTransition(ContainerState.RUNNING,
        ContainerState.EXITED_WITH_SUCCESS,
        ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS,
        new ExitedWithSuccessTransition())
    .addTransition(ContainerState.RUNNING,
        ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
        new ExitedWithFailureTransition())
    .addTransition(ContainerState.RUNNING, ContainerState.RUNNING,
       ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
       UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.RUNNING, ContainerState.KILLING,
        ContainerEventType.KILL_CONTAINER, new KillTransition())

    // From CONTAINER_EXITED_WITH_SUCCESS State
    .addTransition(ContainerState.EXITED_WITH_SUCCESS, ContainerState.DONE,
        ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
        CONTAINER_DONE_TRANSITION)
    .addTransition(ContainerState.EXITED_WITH_SUCCESS,
        ContainerState.EXITED_WITH_SUCCESS,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.EXITED_WITH_SUCCESS,
        ContainerState.EXITED_WITH_SUCCESS,
        ContainerEventType.KILL_CONTAINER)

    // From EXITED_WITH_FAILURE State
    .addTransition(ContainerState.EXITED_WITH_FAILURE, ContainerState.DONE,
            ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
            CONTAINER_DONE_TRANSITION)
    .addTransition(ContainerState.EXITED_WITH_FAILURE,
        ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.EXITED_WITH_FAILURE,
                   ContainerState.EXITED_WITH_FAILURE,
                   ContainerEventType.KILL_CONTAINER)

    // From KILLING State.
    .addTransition(ContainerState.KILLING,
        ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
        ContainerEventType.CONTAINER_KILLED_ON_REQUEST,
        new ContainerKilledTransition())
    .addTransition(ContainerState.KILLING,
        ContainerState.KILLING,
        ContainerEventType.RESOURCE_LOCALIZED,
        new LocalizedResourceDuringKillTransition())
    .addTransition(ContainerState.KILLING, ContainerState.KILLING,
       ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
       UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.KILLING, ContainerState.KILLING,
        ContainerEventType.KILL_CONTAINER)
    .addTransition(ContainerState.KILLING, ContainerState.EXITED_WITH_SUCCESS,
        ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS,
        new ExitedWithSuccessTransition())
    .addTransition(ContainerState.KILLING, ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
        new ExitedWithFailureTransition())
    .addTransition(ContainerState.KILLING,
            ContainerState.DONE,
            ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
            CONTAINER_DONE_TRANSITION)

    // From CONTAINER_CLEANEDUP_AFTER_KILL State.
    .addTransition(ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
            ContainerState.DONE,
            ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
            CONTAINER_DONE_TRANSITION)
    .addTransition(ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
        ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
        ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
        ContainerEventType.KILL_CONTAINER)

    // From DONE
    .addTransition(ContainerState.DONE, ContainerState.DONE,
        ContainerEventType.KILL_CONTAINER)
    .addTransition(ContainerState.DONE, ContainerState.DONE,
       ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
       UPDATE_DIAGNOSTICS_TRANSITION)

    // create the topology tables
    .installTopology();

  private final StateMachine<ContainerState, ContainerEventType, ContainerEvent>
    stateMachine;

  private org.apache.hadoop.yarn.api.records.ContainerState getCurrentState() {
    switch (stateMachine.getCurrentState()) {
    case NEW:
    case LOCALIZING:
    case LOCALIZATION_FAILED:
    case LOCALIZED:
    case RUNNING:
    case EXITED_WITH_SUCCESS:
    case EXITED_WITH_FAILURE:
    case KILLING:
    case CONTAINER_CLEANEDUP_AFTER_KILL:
    case CONTAINER_RESOURCES_CLEANINGUP:
      return org.apache.hadoop.yarn.api.records.ContainerState.RUNNING;
    case DONE:
    default:
      return org.apache.hadoop.yarn.api.records.ContainerState.COMPLETE;
    }
  }

  @Override
  public ContainerId getContainerID() {
    this.readLock.lock();
    try {
      return this.launchContext.getContainerId();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getUser() {
    this.readLock.lock();
    try {
      return this.launchContext.getUser();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Map<Path,String> getLocalizedResources() {
    this.readLock.lock();
    try {
    assert ContainerState.LOCALIZED == getContainerState(); // TODO: FIXME!!
    return localizedResources;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Credentials getCredentials() {
    this.readLock.lock();
    try {
      return credentials;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public ContainerState getContainerState() {
    this.readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public
      org.apache.hadoop.yarn.api.records.Container cloneAndGetContainer() {
    this.readLock.lock();
    try {
      org.apache.hadoop.yarn.api.records.Container c =
        recordFactory.newRecordInstance(
            org.apache.hadoop.yarn.api.records.Container.class);
      c.setId(this.launchContext.getContainerId());
      c.setResource(this.launchContext.getResource());
      c.setState(getCurrentState());
      c.setContainerStatus(cloneAndGetContainerStatus());
      return c;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public ContainerLaunchContext getLaunchContext() {
    this.readLock.lock();
    try {
      return launchContext;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public ContainerStatus cloneAndGetContainerStatus() {
    this.readLock.lock();
    try {
      ContainerStatus containerStatus =
          recordFactory.newRecordInstance(ContainerStatus.class);
      containerStatus.setState(getCurrentState());
      containerStatus.setContainerId(this.launchContext.getContainerId());
      containerStatus.setDiagnostics(diagnostics.toString());
  	  containerStatus.setExitStatus(String.valueOf(exitCode));
      return containerStatus;
    } finally {
      this.readLock.unlock();
    }
  }

  @SuppressWarnings("fallthrough")
  private void finished() {
    switch (getContainerState()) {
      case EXITED_WITH_SUCCESS:
        metrics.endRunningContainer();
        metrics.completedContainer();
        break;
      case EXITED_WITH_FAILURE:
        metrics.endRunningContainer();
        // fall through
      case LOCALIZATION_FAILED:
        metrics.failedContainer();
        break;
      case CONTAINER_CLEANEDUP_AFTER_KILL:
        metrics.endRunningContainer();
        // fall through
      case NEW:
        metrics.killedContainer();
    }

    metrics.releaseContainer(getLaunchContext().getResource());

    // Inform the application
    ContainerId containerID = getContainerID();
    EventHandler eventHandler = dispatcher.getEventHandler();
    eventHandler.handle(new ApplicationContainerFinishedEvent(containerID));
    // Remove the container from the resource-monitor
    eventHandler.handle(new ContainerStopMonitoringEvent(containerID));
    // Tell the logService too
    eventHandler.handle(new LogAggregatorContainerFinishedEvent(
        containerID, exitCode));
  }

  static class ContainerTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {

    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // Just drain the event and change the state.
    }

  }

  @SuppressWarnings("unchecked") // dispatcher not typed
  static class RequestResourcesTransition implements
      MultipleArcTransition<ContainerImpl,ContainerEvent,ContainerState> {
    @Override
    public ContainerState transition(ContainerImpl container,
        ContainerEvent event) {
      final ContainerLaunchContext ctxt = container.getLaunchContext();
      container.metrics.initingContainer();

      // Inform the AuxServices about the opaque serviceData
      Map<String,ByteBuffer> csd = ctxt.getAllServiceData();
      if (csd != null) {
        // This can happen more than once per Application as each container may
        // have distinct service data
        for (Map.Entry<String,ByteBuffer> service : csd.entrySet()) {
          container.dispatcher.getEventHandler().handle(
              new AuxServicesEvent(AuxServicesEventType.APPLICATION_INIT,
                ctxt.getUser(), ctxt.getContainerId().getAppId(),
                service.getKey().toString(), service.getValue()));
        }
      }

      // Send requests for public, private resources
      Map<String,LocalResource> cntrRsrc = ctxt.getAllLocalResources();
      if (!cntrRsrc.isEmpty()) {
        ArrayList<LocalResourceRequest> publicRsrc =
          new ArrayList<LocalResourceRequest>();
        ArrayList<LocalResourceRequest> privateRsrc =
          new ArrayList<LocalResourceRequest>();
        ArrayList<LocalResourceRequest> appRsrc =
          new ArrayList<LocalResourceRequest>();
        try {
          for (Map.Entry<String,LocalResource> rsrc : cntrRsrc.entrySet()) {
            try {
            LocalResourceRequest req =
              new LocalResourceRequest(rsrc.getValue());
            container.pendingResources.put(req, rsrc.getKey());
            switch (rsrc.getValue().getVisibility()) {
            case PUBLIC:
              publicRsrc.add(req);
              break;
            case PRIVATE:
              privateRsrc.add(req);
              break;
            case APPLICATION:
              appRsrc.add(req);
              break;
            }
            } catch (URISyntaxException e) {
              LOG.info("Got exception parsing " + rsrc.getKey()
                  + " and value " + rsrc.getValue());
              throw e;
            }
          }
        } catch (URISyntaxException e) {
          // malformed resource; abort container launch
          LOG.warn("Failed to parse resource-request", e);
          container.dispatcher.getEventHandler().handle(
              new ContainerLocalizationEvent(
               LocalizationEventType.CLEANUP_CONTAINER_RESOURCES, container));
          container.metrics.endInitingContainer();
          return ContainerState.LOCALIZATION_FAILED;
        }
        if (!publicRsrc.isEmpty()) {
          container.dispatcher.getEventHandler().handle(
              new ContainerLocalizationRequestEvent(
                container, publicRsrc, LocalResourceVisibility.PUBLIC));
        }
        if (!privateRsrc.isEmpty()) {
          container.dispatcher.getEventHandler().handle(
              new ContainerLocalizationRequestEvent(
                container, privateRsrc, LocalResourceVisibility.PRIVATE));
        }
        if (!appRsrc.isEmpty()) {
          container.dispatcher.getEventHandler().handle(
              new ContainerLocalizationRequestEvent(
                container, appRsrc, LocalResourceVisibility.APPLICATION));
        }
        return ContainerState.LOCALIZING;
      } else {
        container.dispatcher.getEventHandler().handle(
            new ContainersLauncherEvent(container,
                ContainersLauncherEventType.LAUNCH_CONTAINER));
        container.metrics.endInitingContainer();
        return ContainerState.LOCALIZED;
      }
    }
  }

  @SuppressWarnings("unchecked") // dispatcher not typed
  static class LocalizedTransition implements
      MultipleArcTransition<ContainerImpl,ContainerEvent,ContainerState> {
    @Override
    public ContainerState transition(ContainerImpl container,
        ContainerEvent event) {
      ContainerResourceLocalizedEvent rsrcEvent = (ContainerResourceLocalizedEvent) event;
      String sym = container.pendingResources.remove(rsrcEvent.getResource());
      if (null == sym) {
        LOG.warn("Localized unknown resource " + rsrcEvent.getResource() +
                 " for container " + container.getContainerID());
        assert false;
        // fail container?
        return ContainerState.LOCALIZING;
      }
      container.localizedResources.put(rsrcEvent.getLocation(), sym);
      if (!container.pendingResources.isEmpty()) {
        return ContainerState.LOCALIZING;
      }
      container.dispatcher.getEventHandler().handle(
          new ContainersLauncherEvent(container,
              ContainersLauncherEventType.LAUNCH_CONTAINER));
      container.metrics.endInitingContainer();
      return ContainerState.LOCALIZED;
    }
  }

  @SuppressWarnings("unchecked") // dispatcher not typed
  static class LaunchTransition extends ContainerTransition {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // Inform the ContainersMonitor to start monitoring the container's
      // resource usage.
      // TODO: Fix pmem limits below
      long vmemBytes =
          container.getLaunchContext().getResource().getMemory() * 1024 * 1024L;
      container.dispatcher.getEventHandler().handle(
          new ContainerStartMonitoringEvent(container.getContainerID(),
              vmemBytes, -1));
      container.metrics.runningContainer();
    }
  }

  @SuppressWarnings("unchecked") // dispatcher not typed
  static class ExitedWithSuccessTransition extends ContainerTransition {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // TODO: Add containerWorkDir to the deletion service.

      // Inform the localizer to decrement reference counts and cleanup
      // resources.
      container.dispatcher.getEventHandler().handle(
          new ContainerLocalizationEvent(
            LocalizationEventType.CLEANUP_CONTAINER_RESOURCES, container));
    }
  }

  @SuppressWarnings("unchecked") // dispatcher not typed
  static class ExitedWithFailureTransition extends ContainerTransition {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      ContainerExitEvent exitEvent = (ContainerExitEvent) event;
      container.exitCode = String.valueOf(exitEvent.getExitCode());

      // TODO: Add containerWorkDir to the deletion service.
      // TODO: Add containerOuputDir to the deletion service.

      // Inform the localizer to decrement reference counts and cleanup
      // resources.
      container.dispatcher.getEventHandler().handle(
          new ContainerLocalizationEvent(
            LocalizationEventType.CLEANUP_CONTAINER_RESOURCES, container));
    }
  }

  @SuppressWarnings("unchecked") // dispatcher not typed
  static class ResourceFailedTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {

      ContainerResourceFailedEvent rsrcFailedEvent =
          (ContainerResourceFailedEvent) event;
      container.diagnostics.append(
          StringUtils.stringifyException(rsrcFailedEvent.getCause())).append(
          "\n");

      // Inform the localizer to decrement reference counts and cleanup
      // resources.
      container.dispatcher.getEventHandler().handle(
          new ContainerLocalizationEvent(
            LocalizationEventType.CLEANUP_CONTAINER_RESOURCES, container));
      container.metrics.endInitingContainer();
    }
  }
  
  @SuppressWarnings("unchecked") // dispatcher not typed
  static class KillDuringLocalizationTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // Inform the localizer to decrement reference counts and cleanup
      // resources.
      container.dispatcher.getEventHandler().handle(
          new ContainerLocalizationEvent(
            LocalizationEventType.CLEANUP_CONTAINER_RESOURCES, container));
      container.metrics.endInitingContainer();
      ContainerKillEvent killEvent = (ContainerKillEvent) event;
      container.diagnostics.append(killEvent.getDiagnostic()).append("\n");
    }
  }

  @SuppressWarnings("unchecked") // dispatcher not typed
  static class LocalizedResourceDuringKillTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      ContainerResourceLocalizedEvent rsrcEvent = (ContainerResourceLocalizedEvent) event;
      String sym = container.pendingResources.remove(rsrcEvent.getResource());
      if (null == sym) {
        LOG.warn("Localized unknown resource " + rsrcEvent.getResource() +
                 " for container " + container.getContainerID());
        assert false;
        // fail container?
        return;
      }
      container.localizedResources.put(rsrcEvent.getLocation(), sym);
    }
  }

  @SuppressWarnings("unchecked") // dispatcher not typed
  static class KillTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // Kill the process/process-grp
      container.dispatcher.getEventHandler().handle(
          new ContainersLauncherEvent(container,
              ContainersLauncherEventType.CLEANUP_CONTAINER));
      ContainerKillEvent killEvent = (ContainerKillEvent) event;
      container.diagnostics.append(killEvent.getDiagnostic()).append("\n");
    }
  }

  @SuppressWarnings("unchecked") // dispatcher not typed
  static class ContainerKilledTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      ContainerExitEvent exitEvent = (ContainerExitEvent) event;
      container.exitCode = String.valueOf(exitEvent.getExitCode());

      // The process/process-grp is killed. Decrement reference counts and
      // cleanup resources
      container.dispatcher.getEventHandler().handle(
          new ContainerLocalizationEvent(
            LocalizationEventType.CLEANUP_CONTAINER_RESOURCES, container));
    }
  }

  @SuppressWarnings("unchecked") // dispatcher not typed
  static class ContainerDoneTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      container.finished();
    }
  }
  
  static class ContainerDiagnosticsUpdateTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      ContainerDiagnosticsUpdateEvent updateEvent =
          (ContainerDiagnosticsUpdateEvent) event;
      container.diagnostics.append(updateEvent.getDiagnosticsUpdate())
          .append("\n");
    }
  }

  @Override
  public void handle(ContainerEvent event) {
    try {
      this.writeLock.lock();

      ContainerId containerID = event.getContainerID();
      LOG.info("Processing " + containerID + " of type " + event.getType());

      ContainerState oldState = stateMachine.getCurrentState();
      ContainerState newState = null;
      try {
        newState =
            stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.warn("Can't handle this event at current state", e);
      }
      if (oldState != newState) {
        LOG.info("Container " + containerID + " transitioned from "
            + oldState
            + " to " + newState);
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public String toString() {
    this.readLock.lock();
    try {
      return ConverterUtils.toString(launchContext.getContainerId());
    } finally {
      this.readLock.unlock();
    }
  }

}
