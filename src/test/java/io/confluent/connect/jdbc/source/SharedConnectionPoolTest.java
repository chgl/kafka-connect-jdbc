/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import io.confluent.connect.jdbc.JdbcSourceConnector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class SharedConnectionPoolTest {

  private static final long TIMEOUT_MS = 30_000;

  private EmbeddedDerby db;

  @Before
  public void setup() {
    db = new EmbeddedDerby();
  }

  @After
  public void tearDown() throws Exception {
    assertEquals("A pool was not released", 0, SharedConnectionPool.poolCount());
    db.close();
    db.dropDatabase();
  }

  @Test
  public void shouldSharePoolBetweenConnectorsWithSameConnectionSettings() throws Exception {
    Map<String, String> first = pooledProps();
    first.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "first-");
    Map<String, String> second = pooledProps();
    second.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "second-");
    second.put(JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG, "7");
    second.put(JdbcSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG, "1");
    // Same as the default
    second.put(JdbcSourceConnectorConfig.CONNECTION_POOL_MAX_SIZE_CONFIG, "10");

    SharedConnectionPool pool = SharedConnectionPool.acquire(new JdbcSourceConnectorConfig(first));
    SharedConnectionPool samePool =
        SharedConnectionPool.acquire(new JdbcSourceConnectorConfig(second));

    assertSame(pool, samePool);
    assertEquals(1, SharedConnectionPool.poolCount());

    // The pool has to stay usable for as long as it has a user
    pool.release();
    try (Connection connection = samePool.getConnection()) {
      assertTrue(samePool.isConnectionValid(connection, 5));
      assertEquals(1, samePool.activeConnections());
    }
    assertEquals(0, samePool.activeConnections());

    samePool.release();
    assertEquals(0, SharedConnectionPool.poolCount());
  }

  @Test
  public void shouldNotSharePoolBetweenConnectorsWithDifferentConnectionSettings() {
    Map<String, String> otherUser = pooledProps();
    otherUser.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, "someone");
    Map<String, String> otherDriverProperty = pooledProps();
    otherDriverProperty.put("connection.someDriverProperty", "value");
    Map<String, String> otherSize = pooledProps();
    otherSize.put(JdbcSourceConnectorConfig.CONNECTION_POOL_MAX_SIZE_CONFIG, "3");

    SharedConnectionPool pool =
        SharedConnectionPool.acquire(new JdbcSourceConnectorConfig(pooledProps()));
    SharedConnectionPool otherUserPool =
        SharedConnectionPool.acquire(new JdbcSourceConnectorConfig(otherUser));
    SharedConnectionPool otherDriverPropertyPool =
        SharedConnectionPool.acquire(new JdbcSourceConnectorConfig(otherDriverProperty));
    SharedConnectionPool otherSizePool =
        SharedConnectionPool.acquire(new JdbcSourceConnectorConfig(otherSize));
    try {
      assertNotSame(pool, otherUserPool);
      assertNotSame(pool, otherDriverPropertyPool);
      assertNotSame(pool, otherSizePool);
      assertEquals(4, SharedConnectionPool.poolCount());
    } finally {
      pool.release();
      otherUserPool.release();
      otherDriverPropertyPool.release();
      otherSizePool.release();
    }
  }

  @Test
  public void shouldLetTasksOfDifferentConnectorsShareASingleConnection() throws Exception {
    db.createTable("first", "id", "INT NOT NULL");
    db.insert("first", "id", 1);
    db.createTable("second", "id", "INT NOT NULL");
    db.insert("second", "id", 2);

    // Both tasks can only make progress if each returns the one connection after every query
    JdbcSourceTask firstTask = new JdbcSourceTask();
    JdbcSourceTask secondTask = new JdbcSourceTask();
    SharedConnectionPool pool =
        SharedConnectionPool.acquire(new JdbcSourceTaskConfig(taskProps("first")));
    try {
      firstTask.start(taskProps("first"));
      secondTask.start(taskProps("second"));

      for (int i = 0; i < 3; i++) {
        assertEquals("test-first", pollRecords(firstTask).get(0).topic());
        assertEquals("test-second", pollRecords(secondTask).get(0).topic());
      }
      // Nothing is held on to between queries
      waitFor(() -> pool.activeConnections() == 0);
    } finally {
      firstTask.stop();
      secondTask.stop();
      pool.release();
    }
  }

  @Test
  public void shouldReturnConnectionOfConnectorBetweenTableChecks() throws Exception {
    db.createTable("first", "id", "INT NOT NULL");
    Map<String, String> props = pooledProps();
    props.put(JdbcSourceConnectorConfig.TABLE_POLL_INTERVAL_MS_CONFIG, "50");

    JdbcSourceConnector connector = new JdbcSourceConnector();
    connector.initialize(EasyMock.createNiceMock(ConnectorContext.class));
    SharedConnectionPool pool = SharedConnectionPool.acquire(new JdbcSourceConnectorConfig(props));
    try {
      connector.start(props);
      List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
      assertEquals(1, taskConfigs.size());
      assertEquals("\"APP\".\"first\"", taskConfigs.get(0).get(JdbcSourceTaskConfig.TABLES_CONFIG));
      assertEquals("true", taskConfigs.get(0).get(
          JdbcSourceConnectorConfig.CONNECTION_POOL_ENABLED_CONFIG));

      // The table monitor keeps running, but only borrows a connection while it checks
      for (int i = 0; i < 3; i++) {
        waitFor(() -> pool.activeConnections() == 0);
        Thread.sleep(60);
      }
    } finally {
      connector.stop();
      pool.release();
    }
  }

  @Test
  public void shouldReleasePoolIfConnectorFailsToStart() {
    Map<String, String> props = pooledProps();
    // A query can't be combined with table filters, which is only detected after connecting
    props.put(JdbcSourceConnectorConfig.QUERY_CONFIG, "SELECT * FROM first");
    props.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, "first");

    JdbcSourceConnector connector = new JdbcSourceConnector();
    assertThrows(ConnectException.class, () -> connector.start(props));

    assertEquals(0, SharedConnectionPool.poolCount());
  }

  @Test
  public void shouldNotUsePoolByDefault() throws Exception {
    db.createTable("first", "id", "INT NOT NULL");
    db.insert("first", "id", 1);
    Map<String, String> props = taskProps("first");
    props.remove(JdbcSourceConnectorConfig.CONNECTION_POOL_ENABLED_CONFIG);
    assertFalse(new JdbcSourceTaskConfig(props).connectionPoolEnabled());

    JdbcSourceTask task = new JdbcSourceTask();
    try {
      task.start(props);
      assertEquals("test-first", pollRecords(task).get(0).topic());
      assertEquals(0, SharedConnectionPool.poolCount());
    } finally {
      task.stop();
    }
  }

  private Map<String, String> pooledProps() {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    props.put(JdbcSourceConnectorConfig.CONNECTION_POOL_ENABLED_CONFIG, "true");
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
    return props;
  }

  private Map<String, String> taskProps(String table) {
    Map<String, String> props = pooledProps();
    props.put(JdbcSourceConnectorConfig.CONNECTION_POOL_MAX_SIZE_CONFIG, "1");
    props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, "20");
    props.put(JdbcSourceConnectorConfig.POLL_LINGER_MS_CONFIG, "50");
    props.put(JdbcSourceTaskConfig.TABLES_CONFIG, table);
    props.put(JdbcSourceTaskConfig.TABLES_FETCHED, "true");
    return props;
  }

  private static List<SourceRecord> pollRecords(JdbcSourceTask task) throws Exception {
    long deadline = System.currentTimeMillis() + TIMEOUT_MS;
    while (System.currentTimeMillis() < deadline) {
      List<SourceRecord> records = task.poll();
      if (records != null && !records.isEmpty()) {
        return records;
      }
    }
    throw new AssertionError("Task didn't return any records");
  }

  private static void waitFor(BooleanSupplier condition) throws Exception {
    long deadline = System.currentTimeMillis() + TIMEOUT_MS;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("Condition wasn't met in time");
      }
      Thread.sleep(10);
    }
  }
}
