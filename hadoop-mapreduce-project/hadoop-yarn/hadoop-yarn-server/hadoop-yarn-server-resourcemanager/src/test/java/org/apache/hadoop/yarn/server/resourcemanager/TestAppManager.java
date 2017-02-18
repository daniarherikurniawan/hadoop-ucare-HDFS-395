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

package org.apache.hadoop.yarn.server.resourcemanager;

import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;


import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.security.client.ClientToAMSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMConfig;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.MockRMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemStore;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.service.Service;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.google.common.collect.Maps;
import com.google.common.collect.Lists;

/**
 * Testing applications being retired from RM.
 *
 */

public class TestAppManager{
  private static final Log LOG = LogFactory.getLog(TestAppManager.class);
  private static RMAppEventType appEventType = RMAppEventType.KILL; 

  public synchronized RMAppEventType getAppEventType() {
    return appEventType;
  } 
  public synchronized void setAppEventType(RMAppEventType newType) {
    appEventType = newType;
  } 


  public static List<RMApp> newRMApps(int n, long time, RMAppState state) {
    List<RMApp> list = Lists.newArrayList();
    for (int i = 0; i < n; ++i) {
      list.add(new MockRMApp(i, time, state));
    }
    return list;
  }

  public static RMContext mockRMContext(int n, long time) {
    final List<RMApp> apps = newRMApps(n, time, RMAppState.FINISHED);
    final ConcurrentMap<ApplicationId, RMApp> map = Maps.newConcurrentMap();
    for (RMApp app : apps) {
      map.put(app.getApplicationId(), app);
    }
    Dispatcher rmDispatcher = new AsyncDispatcher();
    ContainerAllocationExpirer containerAllocationExpirer = new ContainerAllocationExpirer(
        rmDispatcher);
    AMLivelinessMonitor amLivelinessMonitor = new AMLivelinessMonitor(
        rmDispatcher);
    return new RMContextImpl(new MemStore(), rmDispatcher,
        containerAllocationExpirer, amLivelinessMonitor) {
      @Override
      public ConcurrentMap<ApplicationId, RMApp> getRMApps() {
        return map;
      }
    };
  }

  public class TestAppManagerDispatcher implements
      EventHandler<RMAppManagerEvent> {

    private final RMContext rmContext;

    public TestAppManagerDispatcher(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(RMAppManagerEvent event) {
       // do nothing
    }   
  }   

  public class TestDispatcher implements
      EventHandler<RMAppEvent> {

    private final RMContext rmContext;

    public TestDispatcher(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(RMAppEvent event) {
      ApplicationId appID = event.getApplicationId();
      //RMApp rmApp = this.rmContext.getRMApps().get(appID);
      setAppEventType(event.getType());
      System.out.println("in handle routine " + getAppEventType().toString());
    }   
  }   
  

  // Extend and make the functions we want to test public
  public class TestRMAppManager extends RMAppManager {

    public TestRMAppManager(RMContext context, Configuration conf) {
      super(context, null, null, null, conf);
      setCompletedAppsMax(RMConfig.DEFAULT_EXPIRE_APPLICATIONS_COMPLETED_MAX);
    }

    public TestRMAppManager(RMContext context, ClientToAMSecretManager
        clientToAMSecretManager, YarnScheduler scheduler,
        ApplicationMasterService masterService, Configuration conf) {
      super(context, clientToAMSecretManager, scheduler, masterService, conf);
      setCompletedAppsMax(RMConfig.DEFAULT_EXPIRE_APPLICATIONS_COMPLETED_MAX);
    }

    public void checkAppNumCompletedLimit() {
      super.checkAppNumCompletedLimit();
    }

    public void addCompletedApp(ApplicationId appId) {
      super.addCompletedApp(appId);
    }

    public int getCompletedAppsListSize() {
      return super.getCompletedAppsListSize();
    }

    public void setCompletedAppsMax(int max) {
      super.setCompletedAppsMax(max);
    }
    public void submitApplication(ApplicationSubmissionContext submissionContext) {
      super.submitApplication(submissionContext);
    }
  }

  protected void addToCompletedApps(TestRMAppManager appMonitor, RMContext rmContext) {
    for (RMApp app : rmContext.getRMApps().values()) {
      if (app.getState() == RMAppState.FINISHED
          || app.getState() == RMAppState.KILLED 
          || app.getState() == RMAppState.FAILED) {
        appMonitor.addCompletedApp(app.getApplicationId());
      }
    }
  }

  @Test
  public void testRMAppRetireNone() throws Exception {
    long now = System.currentTimeMillis();

    // Create such that none of the applications will retire since
    // haven't hit max #
    RMContext rmContext = mockRMContext(10, now - 10);
    TestRMAppManager appMonitor = new TestRMAppManager(rmContext, new Configuration());

    appMonitor.setCompletedAppsMax(10);

    Assert.assertEquals("Number of apps incorrect before checkAppTimeLimit",
        10, rmContext.getRMApps().size());

    // add them to completed apps list
    addToCompletedApps(appMonitor, rmContext);

    // shouldn't  have to many apps
    appMonitor.checkAppNumCompletedLimit();
    Assert.assertEquals("Number of apps incorrect after # completed check", 10,
        rmContext.getRMApps().size());
    Assert.assertEquals("Number of completed apps incorrect after check", 10,
        appMonitor.getCompletedAppsListSize());
  }

  @Test
  public void testRMAppRetireSome() throws Exception {
    long now = System.currentTimeMillis();

    RMContext rmContext = mockRMContext(10, now - 20000);
    TestRMAppManager appMonitor = new TestRMAppManager(rmContext, new Configuration());

    appMonitor.setCompletedAppsMax(3);

    Assert.assertEquals("Number of apps incorrect before", 10, rmContext
        .getRMApps().size());

    // add them to completed apps list
    addToCompletedApps(appMonitor, rmContext);

    // shouldn't  have to many apps
    appMonitor.checkAppNumCompletedLimit();
    Assert.assertEquals("Number of apps incorrect after # completed check", 3,
        rmContext.getRMApps().size());
    Assert.assertEquals("Number of completed apps incorrect after check", 3,
        appMonitor.getCompletedAppsListSize());
  }

  @Test
  public void testRMAppRetireSomeDifferentStates() throws Exception {
    long now = System.currentTimeMillis();

    // these parameters don't matter, override applications below
    RMContext rmContext = mockRMContext(10, now - 20000);
    TestRMAppManager appMonitor = new TestRMAppManager(rmContext, new Configuration());

    appMonitor.setCompletedAppsMax(2);

    // clear out applications map
    rmContext.getRMApps().clear();
    Assert.assertEquals("map isn't empty", 0, rmContext.getRMApps().size());

    // / set with various finished states
    RMApp app = new MockRMApp(0, now - 20000, RMAppState.KILLED);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(1, now - 200000, RMAppState.FAILED);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(2, now - 30000, RMAppState.FINISHED);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(3, now - 20000, RMAppState.RUNNING);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(4, now - 20000, RMAppState.NEW);
    rmContext.getRMApps().put(app.getApplicationId(), app);

    // make sure it doesn't expire these since still running
    app = new MockRMApp(5, now - 10001, RMAppState.KILLED);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(6, now - 30000, RMAppState.ACCEPTED);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(7, now - 20000, RMAppState.SUBMITTED);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(8, now - 10001, RMAppState.FAILED);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(9, now - 20000, RMAppState.FAILED);
    rmContext.getRMApps().put(app.getApplicationId(), app);

    Assert.assertEquals("Number of apps incorrect before", 10, rmContext
        .getRMApps().size());

    // add them to completed apps list
    addToCompletedApps(appMonitor, rmContext);

    // shouldn't  have to many apps
    appMonitor.checkAppNumCompletedLimit();
    Assert.assertEquals("Number of apps incorrect after # completed check", 6,
        rmContext.getRMApps().size());
    Assert.assertEquals("Number of completed apps incorrect after check", 2,
        appMonitor.getCompletedAppsListSize());

  }

  @Test
  public void testRMAppRetireNullApp() throws Exception {
    long now = System.currentTimeMillis();

    RMContext rmContext = mockRMContext(10, now - 20000);
    TestRMAppManager appMonitor = new TestRMAppManager(rmContext, new Configuration());

    Assert.assertEquals("Number of apps incorrect before", 10, rmContext
        .getRMApps().size());

    appMonitor.addCompletedApp(null);

    Assert.assertEquals("Number of completed apps incorrect after check", 0,
        appMonitor.getCompletedAppsListSize());
  }

  @Test
  public void testRMAppRetireZeroSetting() throws Exception {
    long now = System.currentTimeMillis();

    RMContext rmContext = mockRMContext(10, now - 20000);
    TestRMAppManager appMonitor = new TestRMAppManager(rmContext, new Configuration());

    Assert.assertEquals("Number of apps incorrect before", 10, rmContext
        .getRMApps().size());

    // test with 0
    appMonitor.setCompletedAppsMax(0);

    addToCompletedApps(appMonitor, rmContext);
    Assert.assertEquals("Number of completed apps incorrect", 10,
        appMonitor.getCompletedAppsListSize());

    appMonitor.checkAppNumCompletedLimit();

    Assert.assertEquals("Number of apps incorrect after # completed check", 0,
        rmContext.getRMApps().size());
    Assert.assertEquals("Number of completed apps incorrect after check", 0,
        appMonitor.getCompletedAppsListSize());
  }

  protected void setupDispatcher(RMContext rmContext, Configuration conf) {
    TestDispatcher testDispatcher = new TestDispatcher(rmContext);
    TestAppManagerDispatcher testAppManagerDispatcher = new TestAppManagerDispatcher(rmContext);
    rmContext.getDispatcher().register(RMAppEventType.class, testDispatcher);
    rmContext.getDispatcher().register(RMAppManagerEventType.class, testAppManagerDispatcher);
    ((Service)rmContext.getDispatcher()).init(conf);
    ((Service)rmContext.getDispatcher()).start();
    Assert.assertEquals("app event type is wrong before", RMAppEventType.KILL, appEventType);
  }

  @Test
  public void testRMAppSubmit() throws Exception {
    long now = System.currentTimeMillis();

    RMContext rmContext = mockRMContext(0, now - 10);
    ResourceScheduler scheduler = new CapacityScheduler();
    ApplicationMasterService masterService =  new ApplicationMasterService(rmContext,
        new ApplicationTokenSecretManager(), scheduler);
    Configuration conf = new Configuration();
    TestRMAppManager appMonitor = new TestRMAppManager(rmContext, 
        new ClientToAMSecretManager(), scheduler, masterService, conf);

    ApplicationId appID = MockApps.newAppID(1);
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    ApplicationSubmissionContext context = recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    context.setApplicationId(appID);
    setupDispatcher(rmContext, conf);

    appMonitor.submitApplication(context);
    RMApp app = rmContext.getRMApps().get(appID);
    Assert.assertNotNull("app is null", app);
    Assert.assertEquals("app id doesn't match", appID, app.getApplicationId());
    Assert.assertEquals("app name doesn't match", "N/A", app.getName());
    Assert.assertEquals("app queue doesn't match", "default", app.getQueue());
    Assert.assertEquals("app state doesn't match", RMAppState.NEW, app.getState());
    Assert.assertNotNull("app store is null", app.getApplicationStore());

    // wait for event to be processed
    int timeoutSecs = 0;
    while ((getAppEventType() == RMAppEventType.KILL) && 
        timeoutSecs++ < 20) {
      Thread.sleep(1000);
    }
    Assert.assertEquals("app event type sent is wrong", RMAppEventType.START, getAppEventType());
    setAppEventType(RMAppEventType.KILL); 
    ((Service)rmContext.getDispatcher()).stop();
  }

  @Test
  public void testRMAppSubmitWithQueueAndName() throws Exception {
    long now = System.currentTimeMillis();

    RMContext rmContext = mockRMContext(1, now - 10);
    ResourceScheduler scheduler = new CapacityScheduler();
    ApplicationMasterService masterService =  new ApplicationMasterService(rmContext,
        new ApplicationTokenSecretManager(), scheduler);
    Configuration conf = new Configuration();
    TestRMAppManager appMonitor = new TestRMAppManager(rmContext, 
        new ClientToAMSecretManager(), scheduler, masterService, conf);

    ApplicationId appID = MockApps.newAppID(10);
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    ApplicationSubmissionContext context = recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    context.setApplicationId(appID);
    context.setApplicationName("testApp1");
    context.setQueue("testQueue");

    setupDispatcher(rmContext, conf);

    appMonitor.submitApplication(context);
    RMApp app = rmContext.getRMApps().get(appID);
    Assert.assertNotNull("app is null", app);
    Assert.assertEquals("app id doesn't match", appID, app.getApplicationId());
    Assert.assertEquals("app name doesn't match", "testApp1", app.getName());
    Assert.assertEquals("app queue doesn't match", "testQueue", app.getQueue());
    Assert.assertEquals("app state doesn't match", RMAppState.NEW, app.getState());
    Assert.assertNotNull("app store is null", app.getApplicationStore());

    // wait for event to be processed
    int timeoutSecs = 0;
    while ((getAppEventType() == RMAppEventType.KILL) && 
        timeoutSecs++ < 20) {
      Thread.sleep(1000);
    }
    Assert.assertEquals("app event type sent is wrong", RMAppEventType.START, getAppEventType());
    setAppEventType(RMAppEventType.KILL); 
    ((Service)rmContext.getDispatcher()).stop();
  }

  @Test
  public void testRMAppSubmitError() throws Exception {
    long now = System.currentTimeMillis();

    // specify 1 here and use same appId below so it gets duplicate entry
    RMContext rmContext = mockRMContext(1, now - 10);
    ResourceScheduler scheduler = new CapacityScheduler();
    ApplicationMasterService masterService =  new ApplicationMasterService(rmContext,
        new ApplicationTokenSecretManager(), scheduler);
    Configuration conf = new Configuration();
    TestRMAppManager appMonitor = new TestRMAppManager(rmContext, 
        new ClientToAMSecretManager(), scheduler, masterService, conf);

    ApplicationId appID = MockApps.newAppID(0);
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    ApplicationSubmissionContext context = recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    context.setApplicationId(appID);
    context.setApplicationName("testApp1");
    context.setQueue("testQueue");

    setupDispatcher(rmContext, conf);

    RMApp appOrig = rmContext.getRMApps().get(appID);
    Assert.assertTrue("app name matches but shouldn't", "testApp1" != appOrig.getName());

    // our testApp1 should be rejected and original app with same id should be left in place
    appMonitor.submitApplication(context);

    // make sure original app didn't get removed
    RMApp app = rmContext.getRMApps().get(appID);
    Assert.assertNotNull("app is null", app);
    Assert.assertEquals("app id doesn't match", appID, app.getApplicationId());
    Assert.assertEquals("app name doesn't matches", appOrig.getName(), app.getName());
    ((Service)rmContext.getDispatcher()).stop();
  }

}
