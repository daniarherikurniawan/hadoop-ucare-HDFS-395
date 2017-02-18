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

package org.apache.hadoop.mapreduce.v2.app.job;

import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.yarn.api.records.ContainerId;


/**
 * Read only view of TaskAttempt.
 */
public interface TaskAttempt {
  TaskAttemptId getID();
  TaskAttemptReport getReport();
  List<String> getDiagnostics();
  Counters getCounters();
  float getProgress();
  TaskAttemptState getState();

  /** Has attempt reached the final state or not.
   */
  boolean isFinished();

  /**If container Assigned then return container ID, otherwise null.
   */
  ContainerId getAssignedContainerID();

  /**If container Assigned then return container mgr address, otherwise null.
   */
  String getAssignedContainerMgrAddress();
  
  /**If container Assigned then return the node's http address, otherwise null.
   */
  String getNodeHttpAddress();

  /** Returns time at which container is launched. If container is not launched
   * yet, returns 0.
   */
  long getLaunchTime();

  /** Returns attempt's finish time. If attempt is not finished
   *  yet, returns 0.
   */
  long getFinishTime();
}
