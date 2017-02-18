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

package org.apache.hadoop.yarn.ipc;

import java.net.InetSocketAddress;

import org.apache.avro.ipc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factory.providers.RpcFactoryProvider;

/**
 * This uses Hadoop RPC. Uses a tunnel ProtoSpecificRpcEngine over 
 * Hadoop connection.
 * This does not give cross-language wire compatibility, since the Hadoop 
 * RPC wire format is non-standard, but it does permit use of Protocol Buffers
 *  protocol versioning features for inter-Java RPCs.
 */
public class HadoopYarnProtoRPC extends YarnRPC {

  private static final Log LOG = LogFactory.getLog(HadoopYarnRPC.class);

  @Override
  public Object getProxy(Class protocol, InetSocketAddress addr,
      Configuration conf) {
    Configuration myConf = new Configuration(conf);
    LOG.info("Creating a HadoopYarnProtoRpc proxy for protocol " + protocol);
    LOG.debug("Configured SecurityInfo class name is "
        + myConf.get(YarnConfiguration.YARN_SECURITY_INFO));
    
    return RpcFactoryProvider.getClientFactory(myConf).getClient(protocol, 1, addr, myConf);
  }

  @Override
  public Server getServer(Class protocol, Object instance,
      InetSocketAddress addr, Configuration conf,
      SecretManager<? extends TokenIdentifier> secretManager,
      int numHandlers) {
    LOG.info("Creating a HadoopYarnProtoRpc server for protocol " + protocol + 
        " with " + numHandlers + " handlers");
    LOG.info("Configured SecurityInfo class name is "
        + conf.get(YarnConfiguration.YARN_SECURITY_INFO));
    
    final RPC.Server hadoopServer;
    hadoopServer = 
      RpcFactoryProvider.getServerFactory(conf).getServer(protocol, instance, 
          addr, conf, secretManager, numHandlers);

    Server server = new Server() {
      @Override
      public void close() {
        hadoopServer.stop();
      }

      @Override
      public int getPort() {
        return hadoopServer.getListenerAddress().getPort();
      }

      @Override
      public void join() throws InterruptedException {
        hadoopServer.join();
      }

      @Override
      public void start() {
        hadoopServer.start();
      }
    };
    return server;

  }

}
