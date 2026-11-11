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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.util.ConnectionProvider;

/**
 * A pool of JDBC connections that is shared by all source connectors and tasks running in this
 * JVM (i.e. in one Kafka Connect worker) whose connection settings are identical.
 *
 * <p>Pools are reference counted: every {@link #acquire(JdbcSourceConnectorConfig)} must be paired
 * with one {@link #release()}, and the pool is shut down when its last user releases it.
 * Connections obtained from {@link #getConnection()} are handed back to the pool by closing them.
 */
public class SharedConnectionPool implements ConnectionProvider {

  private static final Logger log = LoggerFactory.getLogger(SharedConnectionPool.class);

  // Guarded by the class lock. The keys contain credentials and must never be logged.
  private static final Map<Map<String, Object>, SharedConnectionPool> POOLS = new HashMap<>();
  private static final AtomicInteger POOL_COUNTER = new AtomicInteger();

  private final Map<String, Object> key;
  private final JdbcSourceConnectorConfig config;
  private final String name;
  private final String identifier;
  private final HikariDataSource dataSource;
  private int users; // guarded by the class lock

  /**
   * Get the pool for the given configuration, creating it if this is its first user.
   *
   * @param config the connector or task configuration; may not be null
   * @return the pool; never null
   */
  public static synchronized SharedConnectionPool acquire(JdbcSourceConnectorConfig config) {
    Map<String, Object> key = poolKey(config);
    SharedConnectionPool pool = POOLS.get(key);
    if (pool == null) {
      pool = new SharedConnectionPool(key, config);
      POOLS.put(key, pool);
    }
    pool.users++;
    log.info("Using shared connection pool {} for {}, which now has {} user(s)",
        pool.name, pool.identifier, pool.users);
    return pool;
  }

  /**
   * The settings that determine how connections are opened. Connectors and tasks share a pool
   * if and only if these are equal.
   */
  static Map<String, Object> poolKey(JdbcSourceConnectorConfig config) {
    // Besides the URL and credentials this covers any extra 'connection.*' driver properties
    Map<String, Object> key = new HashMap<>(
        config.originalsWithPrefix(JdbcSourceConnectorConfig.CONNECTION_PREFIX, false)
    );
    key.keySet().removeIf(k -> k.startsWith(JdbcSourceConnectorConfig.CONNECTION_POOL_PREFIX));
    key.remove(JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG);
    key.remove(JdbcSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG);
    key.putAll(config.originalsWithPrefix(
        JdbcSourceConnectorConfig.CREDENTIALS_PROVIDER_CONFIG_PREFIX, false
    ));
    key.put(
        JdbcSourceConnectorConfig.DIALECT_NAME_CONFIG,
        config.getString(JdbcSourceConnectorConfig.DIALECT_NAME_CONFIG)
    );
    key.put(
        JdbcSourceConnectorConfig.CONNECTION_POOL_MAX_SIZE_CONFIG,
        config.connectionPoolMaxSize()
    );
    return key;
  }

  private SharedConnectionPool(Map<String, Object> key, JdbcSourceConnectorConfig config) {
    this.key = key;
    this.config = config;
    this.name = "jdbc-source-pool-" + POOL_COUNTER.incrementAndGet();
    this.identifier = createDialect().identifier();

    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setPoolName(name);
    hikariConfig.setDataSource(new DialectDataSource());
    hikariConfig.setMaximumPoolSize(config.connectionPoolMaxSize());
    // Don't keep connections open that nobody is using
    hikariConfig.setMinimumIdle(0);
    // Don't fail on creation if the database is unreachable; getConnection() will report it and
    // callers already retry that
    hikariConfig.setInitializationFailTimeout(-1);
    this.dataSource = new HikariDataSource(hikariConfig);
    log.info("Created shared connection pool {} for {} with at most {} connection(s)",
        name, identifier, config.connectionPoolMaxSize());
  }

  /**
   * Stop using this pool. The pool is shut down if this was its last user.
   */
  public void release() {
    synchronized (SharedConnectionPool.class) {
      users--;
      log.info("Released shared connection pool {} for {}, which now has {} user(s)",
          name, identifier, users);
      if (users > 0) {
        return;
      }
      POOLS.remove(key);
    }
    log.info("Closing shared connection pool {} for {}", name, identifier);
    dataSource.close();
  }

  /**
   * Borrow a connection from the pool, waiting for one to become available if necessary. The
   * connection must be closed to return it to the pool.
   */
  @Override
  public Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  @Override
  public boolean isConnectionValid(Connection connection, int timeout) throws SQLException {
    return connection.isValid(timeout);
  }

  /**
   * Does nothing: individual connections are returned by closing them, and the pool itself is
   * given up with {@link #release()}.
   */
  @Override
  public void close() {
  }

  @Override
  public String identifier() {
    return identifier;
  }

  @Override
  public String toString() {
    return name + " for " + identifier;
  }

  // Visible for testing
  int activeConnections() {
    return dataSource.getHikariPoolMXBean().getActiveConnections();
  }

  // Visible for testing
  static synchronized int poolCount() {
    return POOLS.size();
  }

  private DatabaseDialect createDialect() {
    String dialectName = config.getString(JdbcSourceConnectorConfig.DIALECT_NAME_CONFIG);
    if (dialectName != null && !dialectName.trim().isEmpty()) {
      return DatabaseDialects.create(dialectName, config);
    }
    return DatabaseDialects.findBestFor(
        config.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG),
        config
    );
  }

  /**
   * Opens the pool's physical connections through the dialect, so that they are set up exactly
   * like the connections of connectors that don't use a pool.
   */
  private class DialectDataSource implements DataSource {

    @Override
    public Connection getConnection() throws SQLException {
      // A dialect keeps a reference to every connection it has opened until the dialect is
      // closed, which also closes those connections. Use a throwaway dialect per connection so
      // that connections retired by the pool don't pile up.
      return createDialect().getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PrintWriter getLogWriter() {
      return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) {
    }

    @Override
    public void setLoginTimeout(int seconds) {
    }

    @Override
    public int getLoginTimeout() {
      return 0;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return false;
    }
  }
}
