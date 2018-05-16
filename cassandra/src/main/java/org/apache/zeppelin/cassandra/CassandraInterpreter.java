/*
 * Forked from original Zeppelin code by Alexey Ott. All made changes are
 * copyrighted by DataStax, 2018
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.cassandra;

import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.lang.Integer.parseInt;

/**
 * Interpreter for Apache Cassandra CQL query language
 */
public class CassandraInterpreter extends Interpreter {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraInterpreter.class);

  private static final String CASSANDRA_INTERPRETER_PARALLELISM =
          "cassandra.interpreter.parallelism";
  static final String CASSANDRA_PORT = "cassandra.native.port";
  static final String CASSANDRA_HOSTS = "cassandra.hosts";
  public static final String CASSANDRA_PROTOCOL_VERSION = "cassandra.protocol.version";
  static final String CASSANDRA_CLUSTER_NAME = "cassandra.cluster";
  public static final String CASSANDRA_COMPRESSION_PROTOCOL = "cassandra.compression.protocol";
  static final String CASSANDRA_CREDENTIALS_USERNAME = "cassandra.credentials.username";
  static final String CASSANDRA_CREDENTIALS_PASSWORD = "cassandra.credentials.password";
  public static final String CASSANDRA_LOAD_BALANCING_POLICY = "cassandra.load.balancing.policy";
  public static final String CASSANDRA_RETRY_POLICY = "cassandra.retry.policy";
  public static final String CASSANDRA_RECONNECTION_POLICY = "cassandra.reconnection.policy";
  public static final String CASSANDRA_SPECULATIVE_EXECUTION_POLICY =
          "cassandra.speculative.execution.policy";
  static final String CASSANDRA_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS =
          "cassandra.max.schema.agreement.wait.second";
  public static final String CASSANDRA_POOLING_NEW_CONNECTION_THRESHOLD_LOCAL =
          "cassandra.pooling.new.connection.threshold.local";
  public static final String CASSANDRA_POOLING_NEW_CONNECTION_THRESHOLD_REMOTE =
          "cassandra.pooling.new.connection.threshold.remote";
  public static final String CASSANDRA_POOLING_MAX_CONNECTION_PER_HOST_LOCAL =
          "cassandra.pooling.max.connection.per.host.local";
  public static final String CASSANDRA_POOLING_MAX_CONNECTION_PER_HOST_REMOTE =
          "cassandra.pooling.max.connection.per.host.remote";
  public static final String CASSANDRA_POOLING_CORE_CONNECTION_PER_HOST_LOCAL =
          "cassandra.pooling.core.connection.per.host.local";
  public static final String CASSANDRA_POOLING_CORE_CONNECTION_PER_HOST_REMOTE =
          "cassandra.pooling.core.connection.per.host.remote";
  public static final String CASSANDRA_POOLING_MAX_REQUESTS_PER_CONNECTION_LOCAL =
          "cassandra.pooling.max.request.per.connection.local";
  public static final String CASSANDRA_POOLING_MAX_REQUESTS_PER_CONNECTION_REMOTE =
          "cassandra.pooling.max.request.per.connection.remote";
  public static final String CASSANDRA_POOLING_IDLE_TIMEOUT_SECONDS =
          "cassandra.pooling.idle.timeout.seconds";
  public static final String CASSANDRA_POOLING_POOL_TIMEOUT_MILLIS =
          "cassandra.pooling.pool.timeout.millisecs";
  public static final String CASSANDRA_POOLING_HEARTBEAT_INTERVAL_SECONDS =
          "cassandra.pooling.heartbeat.interval.seconds";
  public static final String CASSANDRA_QUERY_DEFAULT_CONSISTENCY =
          "cassandra.query.default.consistency";
  public static final String CASSANDRA_QUERY_DEFAULT_SERIAL_CONSISTENCY =
          "cassandra.query.default.serial.consistency";
  public static final String CASSANDRA_QUERY_DEFAULT_FETCH_SIZE =
          "cassandra.query.default.fetchSize";
  public static final String CASSANDRA_QUERY_DEFAULT_IDEMPOTENCE =
          "cassandra.query.default.idempotence";
  public static final String CASSANDRA_SOCKET_CONNECTION_TIMEOUT_MILLIS =
          "cassandra.socket.connection.timeout.millisecs";
  public static final String CASSANDRA_SOCKET_KEEP_ALIVE =
          "cassandra.socket.keep.alive";
  public static final String CASSANDRA_SOCKET_READ_TIMEOUT_MILLIS =
          "cassandra.socket.read.timeout.millisecs";
  public static final String CASSANDRA_SOCKET_RECEIVED_BUFFER_SIZE_BYTES =
          "cassandra.socket.received.buffer.size.bytes";
  public static final String CASSANDRA_SOCKET_REUSE_ADDRESS =
          "cassandra.socket.reuse.address";
  public static final String CASSANDRA_SOCKET_SEND_BUFFER_SIZE_BYTES =
          "cassandra.socket.send.buffer.size.bytes";
  public static final String CASSANDRA_SOCKET_SO_LINGER =
          "cassandra.socket.soLinger";
  public static final String CASSANDRA_SOCKET_TCP_NO_DELAY =
          "cassandra.socket.tcp.no_delay";
  private static final String CASSANDRA_WITH_SSL =
          "cassandra.ssl.enabled";
  private static final String CASSANDRA_TRUSTSTORE_PATH =
          "cassandra.ssl.truststore.path";
  private static final String CASSANDRA_TRUSTSTORE_PASSWORD =
          "cassandra.ssl.truststore.password";


  public static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_PORT = "9042";
  public static final String DEFAULT_CLUSTER = "Test Cluster";
  public static final String DEFAULT_KEYSPACE = "system";
  public static final String DEFAULT_PROTOCOL_VERSION = "4";
  public static final String DEFAULT_COMPRESSION = "NONE";
  public static final String DEFAULT_CREDENTIAL = "none";
  public static final String DEFAULT_POLICY = "DEFAULT";
  public static final String DEFAULT_PARALLELISM = "10";
  static String DEFAULT_NEW_CONNECTION_THRESHOLD_LOCAL = "100";
  static String DEFAULT_NEW_CONNECTION_THRESHOLD_REMOTE = "100";
  static String DEFAULT_CORE_CONNECTION_PER_HOST_LOCAL = "2";
  static String DEFAULT_CORE_CONNECTION_PER_HOST_REMOTE = "1";
  static String DEFAULT_MAX_CONNECTION_PER_HOST_LOCAL = "8";
  static String DEFAULT_MAX_CONNECTION_PER_HOST_REMOTE = "2";
  static String DEFAULT_MAX_REQUEST_PER_CONNECTION_LOCAL = "1024";
  static String DEFAULT_MAX_REQUEST_PER_CONNECTION_REMOTE = "256";
  public static final String DEFAULT_IDLE_TIMEOUT = "120";
  public static final String DEFAULT_POOL_TIMEOUT = "5000";
  public static final String DEFAULT_HEARTBEAT_INTERVAL = "30";
  public static final String DEFAULT_CONSISTENCY = "ONE";
  public static final String DEFAULT_SERIAL_CONSISTENCY = "SERIAL";
  public static final String DEFAULT_FETCH_SIZE = "5000";
  public static final String DEFAULT_CONNECTION_TIMEOUT = "5000";
  public static final String DEFAULT_READ_TIMEOUT = "12000";
  public static final String DEFAULT_TCP_NO_DELAY = "true";

  public static final String DOWNGRADING_CONSISTENCY_RETRY = "DOWNGRADING_CONSISTENCY";
  public static final String FALLTHROUGH_RETRY = "FALLTHROUGH";
  public static final String LOGGING_DEFAULT_RETRY = "LOGGING_DEFAULT";
  public static final String LOGGING_DOWNGRADING_RETRY = "LOGGING_DOWNGRADING";
  public static final String LOGGING_FALLTHROUGH_RETRY = "LOGGING_FALLTHROUGH";

  static final List NO_COMPLETION = new ArrayList<>();

  InterpreterLogic helper;
  DseCluster cluster;
  DseSession session;

  private JavaDriverConfig driverConfig = new JavaDriverConfig();

  public CassandraInterpreter(Properties properties) {
    super(properties);
  }

  private void addCredentials(DseCluster.Builder clusterBuilder) {
    String username = getProperty(CASSANDRA_CREDENTIALS_USERNAME);
    String password = getProperty(CASSANDRA_CREDENTIALS_PASSWORD);

    // https://github.com/apache/zeppelin/pull/860/commits/cfe4c8627bb7f4ba57e5ee3369bcbd66e3aba44d
//    AuthenticationInfo authenticationInfo =  this. //contextInterpreter.getAuthenticationInfo();

    clusterBuilder.withCredentials(username, password);
  }

  @Override
  public void open() {

    String hosts = getProperty(CASSANDRA_HOSTS, DEFAULT_HOST);
    final String[] addresses = hosts.split(",");
    final int port = parseInt(getProperty(CASSANDRA_PORT, DEFAULT_PORT));

    LOGGER.info("Bootstrapping Cassandra Java Driver to connect to " + hosts +
            " on port " + port);

    Compression compression = driverConfig.getCompressionProtocol(this);

    DseCluster.Builder clusterBuilder = DseCluster.builder()
            .addContactPoints(addresses)
            .withPort(port)
            .withProtocolVersion(driverConfig.getProtocolVersion(this))
            .withClusterName(getProperty(CASSANDRA_CLUSTER_NAME))
            .withCompression(compression)
            .withLoadBalancingPolicy(driverConfig.getLoadBalancingPolicy(this))
            .withRetryPolicy(driverConfig.getRetryPolicy(this))
            .withReconnectionPolicy(driverConfig.getReconnectionPolicy(this))
            .withSpeculativeExecutionPolicy(driverConfig.getSpeculativeExecutionPolicy(this))
            .withMaxSchemaAgreementWaitSeconds(
                    parseInt(getProperty(CASSANDRA_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS)))
            .withPoolingOptions(driverConfig.getPoolingOptions(this))
            .withQueryOptions(driverConfig.getQueryOptions(this))
            .withSocketOptions(driverConfig.getSocketOptions(this));

    addCredentials(clusterBuilder);

    final String runWithSSL = getProperty(CASSANDRA_WITH_SSL, "false");
    if (runWithSSL.equalsIgnoreCase("true")) {
      LOGGER.debug("Cassandra Interpreter: Using SSL");

      try (InputStream stream = Files.newInputStream(Paths.get(
              getProperty(CASSANDRA_TRUSTSTORE_PATH)))) {
        final SSLContext sslContext;
        {
          final KeyStore trustStore = KeyStore.getInstance("JKS");
          trustStore.load(stream, getProperty(CASSANDRA_TRUSTSTORE_PASSWORD).toCharArray());

          final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
                  TrustManagerFactory.getDefaultAlgorithm());
          trustManagerFactory.init(trustStore);

          sslContext = SSLContext.getInstance("TLS");
          sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
        }
        JdkSSLOptions sslOptions = RemoteEndpointAwareJdkSSLOptions.builder()
                .withSSLContext(sslContext).build();
        clusterBuilder = clusterBuilder.withSSL(sslOptions);
      } catch (Exception e) {
        LOGGER.error(e.toString());
      }
    } else {
      LOGGER.debug("Cassandra Interpreter: Not using SSL");
    }

    cluster = clusterBuilder.build();
    session = cluster.connect();
  }

  DseSession getSession(InterpreterContext context) {
    return session;
  }

  @Override
  public void close() {
    // TODO(alex): iterate over the all stored entries & close sessions & clusters
    session.close();
    cluster.close();
  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) {
    // TODO(alex): put here a call to function that will fetch user-specific session...
    DseSession session = getSession(context);
    return InterpreterLogic.interpret(session, st, context);
  }

  @Override
  public void cancel(InterpreterContext context) {

  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public List<InterpreterCompletion> completion(String buf, int cursor,
      InterpreterContext interpreterContext) {
    return NO_COMPLETION;
  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton()
            .createOrGetParallelScheduler(CassandraInterpreter.class.getName() + this.hashCode(),
                    parseInt(getProperty(CASSANDRA_INTERPRETER_PARALLELISM, DEFAULT_PARALLELISM)));
  }
}
