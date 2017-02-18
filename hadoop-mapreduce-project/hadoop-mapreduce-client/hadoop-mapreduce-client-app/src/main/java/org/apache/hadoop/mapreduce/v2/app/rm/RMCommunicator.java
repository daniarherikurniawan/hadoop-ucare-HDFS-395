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

package org.apache.hadoop.mapreduce.v2.app.rm;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.AMConstants;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.SchedulerSecurityInfo;
import org.apache.hadoop.yarn.service.AbstractService;

/**
 * Registers/unregisters to RM and sends heartbeats to RM.
 */
public class RMCommunicator extends AbstractService  {
  private static final Log LOG = LogFactory.getLog(RMContainerAllocator.class);
  private int rmPollInterval;//millis
  protected ApplicationId applicationId;
  protected ApplicationAttemptId applicationAttemptId;
  private volatile boolean stopped;
  protected Thread allocatorThread;
  protected EventHandler eventHandler;
  protected AMRMProtocol scheduler;
  private final ClientService clientService;
  private int lastResponseID;
  private Resource minContainerCapability;
  private Resource maxContainerCapability;

  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  
  private final AppContext context;
  private Job job;

  public RMCommunicator(ClientService clientService, AppContext context) {
    super("RMCommunicator");
    this.clientService = clientService;
    this.context = context;
    this.eventHandler = context.getEventHandler();
    this.applicationId = context.getApplicationID();
    this.applicationAttemptId = context.getApplicationAttemptId();
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    rmPollInterval =
        conf.getInt(AMConstants.AM_RM_SCHEDULE_INTERVAL,
            AMConstants.DEFAULT_AM_RM_SCHEDULE_INTERVAL);
  }

  @Override
  public void start() {
    scheduler= createSchedulerProxy();
    //LOG.info("Scheduler is " + scheduler);
    register();
    startAllocatorThread();
    JobID id = TypeConverter.fromYarn(context.getApplicationID());
    JobId jobId = TypeConverter.toYarn(id);
    job = context.getJob(jobId);
    super.start();
  }

  protected AppContext getContext() {
    return context;
  }

  protected Job getJob() {
    return job;
  }

  protected void register() {
    //Register
    String host = 
      clientService.getBindAddress().getAddress().getHostAddress();
    try {
      RegisterApplicationMasterRequest request =
        recordFactory.newRecordInstance(RegisterApplicationMasterRequest.class);
      request.setApplicationAttemptId(applicationAttemptId);
      request.setHost(host);
      request.setRpcPort(clientService.getBindAddress().getPort());
      request.setTrackingUrl(host + ":" + clientService.getHttpPort());
      RegisterApplicationMasterResponse response = 
        scheduler.registerApplicationMaster(request);
      minContainerCapability = response.getMinimumResourceCapability();
      maxContainerCapability = response.getMaximumResourceCapability();
      LOG.info("minContainerCapability: " + minContainerCapability.getMemory());
      LOG.info("maxContainerCapability: " + maxContainerCapability.getMemory());
    } catch (Exception are) {
      LOG.info("Exception while registering", are);
      throw new YarnException(are);
    }
  }

  protected void unregister() {
    try {
      String finalState = "RUNNING";
      if (job.getState() == JobState.SUCCEEDED) {
        finalState = "SUCCEEDED";
      } else if (job.getState() == JobState.KILLED) {
        finalState = "KILLED";
      } else if (job.getState() == JobState.FAILED
          || job.getState() == JobState.ERROR) {
        finalState = "FAILED";
      }
      StringBuffer sb = new StringBuffer();
      for (String s : job.getDiagnostics()) {
        sb.append(s).append("\n");
      }
      LOG.info("Setting job diagnostics to " + sb.toString());
      
      String historyUrl = JobHistoryUtils.getHistoryUrl(getConfig(), 
          context.getApplicationID());
      LOG.info("History url is " + historyUrl);

      FinishApplicationMasterRequest request =
          recordFactory.newRecordInstance(FinishApplicationMasterRequest.class);
      request.setAppAttemptId(this.applicationAttemptId);
      request.setFinalState(finalState.toString());
      request.setDiagnostics(sb.toString());
      request.setTrackingUrl(historyUrl);
      scheduler.finishApplicationMaster(request);
    } catch(Exception are) {
      LOG.info("Exception while unregistering ", are);
    }
  }

  protected Resource getMinContainerCapability() {
    return minContainerCapability;
  }
  
  protected Resource getMaxContainerCapability() {
    return maxContainerCapability;
  }

  @Override
  public void stop() {
    stopped = true;
    allocatorThread.interrupt();
    try {
      allocatorThread.join();
    } catch (InterruptedException ie) {
      LOG.info("InterruptedException while stopping", ie);
    }
    unregister();
    super.stop();
  }

  protected void startAllocatorThread() {
    allocatorThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!stopped && !Thread.currentThread().isInterrupted()) {
          try {
            Thread.sleep(rmPollInterval);
            try {
              heartbeat();
            } catch (Exception e) {
              LOG.error("ERROR IN CONTACTING RM. ", e);
              // TODO: for other exceptions
            }
          } catch (InterruptedException e) {
            LOG.info("Allocated thread interrupted. Returning.");
            return;
          }
        }
      }
    });
    allocatorThread.start();
  }

  protected AMRMProtocol createSchedulerProxy() {
    final YarnRPC rpc = YarnRPC.create(getConfig());
    final Configuration conf = new Configuration(getConfig());
    final String serviceAddr = conf.get(
        YarnConfiguration.SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_SCHEDULER_BIND_ADDRESS);

    UserGroupInformation currentUser;
    try {
      currentUser = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new YarnException(e);
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      conf.setClass(YarnConfiguration.YARN_SECURITY_INFO,
          SchedulerSecurityInfo.class, SecurityInfo.class);

      String tokenURLEncodedStr = System.getenv().get(
          ApplicationConstants.APPLICATION_MASTER_TOKEN_ENV_NAME);
      LOG.debug("AppMasterToken is " + tokenURLEncodedStr);
      Token<? extends TokenIdentifier> token = new Token<TokenIdentifier>();

      try {
        token.decodeFromUrlString(tokenURLEncodedStr);
      } catch (IOException e) {
        throw new YarnException(e);
      }

      currentUser.addToken(token);
    }

    return currentUser.doAs(new PrivilegedAction<AMRMProtocol>() {
      @Override
      public AMRMProtocol run() {
        return (AMRMProtocol) rpc.getProxy(AMRMProtocol.class,
            NetUtils.createSocketAddr(serviceAddr), conf);
      }
    });
  }

  protected synchronized void heartbeat() throws Exception {
    AllocateRequest allocateRequest =
        recordFactory.newRecordInstance(AllocateRequest.class);
    allocateRequest.setApplicationAttemptId(applicationAttemptId);
    allocateRequest.setResponseId(lastResponseID);
    allocateRequest.addAllAsks(new ArrayList<ResourceRequest>());
    allocateRequest.addAllReleases(new ArrayList<ContainerId>());
    AllocateResponse allocateResponse = scheduler.allocate(allocateRequest);
    AMResponse response = allocateResponse.getAMResponse();
    if (response.getReboot()) {
      LOG.info("Event from RM: shutting down Application Master");
    }
  }

}
