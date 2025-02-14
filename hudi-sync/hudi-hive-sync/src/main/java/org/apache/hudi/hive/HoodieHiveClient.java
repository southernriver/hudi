/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.hive;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.StorageSchemes;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.hive.util.HiveSchemaUtil;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hudi.sync.common.AbstractSyncHoodieClient;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;
import org.apache.thrift.TException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HoodieHiveClient extends AbstractSyncHoodieClient {

  private static final String HOODIE_LAST_COMMIT_TIME_SYNC = "last_commit_time_sync";
  private static final String HIVE_ESCAPE_CHARACTER = HiveSchemaUtil.HIVE_ESCAPE_CHARACTER;

  private static final Logger LOG = LogManager.getLogger(HoodieHiveClient.class);
  private final PartitionValueExtractor partitionValueExtractor;
  private IMetaStoreClient client;
  private SessionState sessionState;
  private Driver hiveDriver;
  private HiveSyncConfig syncConfig;
  private FileSystem fs;
  private Connection connection;
  private HoodieTimeline activeTimeline;
  private HiveConf configuration;

  public HoodieHiveClient(HiveSyncConfig cfg, HiveConf configuration, FileSystem fs) {
    super(cfg.basePath, cfg.assumeDatePartitioning, cfg.useFileListingFromMetadata, cfg.verifyMetadataFileListing, fs);
    this.syncConfig = cfg;
    this.fs = fs;

    this.configuration = configuration;
    // Support both JDBC and metastore based implementations for backwards compatiblity. Future users should
    // disable jdbc and depend on metastore client for all hive registrations
    if (cfg.useJdbc) {
      LOG.info("Creating hive connection " + cfg.jdbcUrl);
      createHiveConnection();
    }
    try {
      HoodieTimer timer = new HoodieTimer().startTimer();
      this.sessionState = new SessionState(configuration,
              UserGroupInformation.getCurrentUser().getShortUserName());
      SessionState.start(this.sessionState);
      this.sessionState.setCurrentDatabase(syncConfig.databaseName);
      hiveDriver = new org.apache.hadoop.hive.ql.Driver(configuration);
      this.client = Hive.get(configuration).getMSC();
      LOG.info(String.format("Time taken to start SessionState and create Driver and client: "
              + "%s ms", (timer.endTimer())));
    } catch (MetaException | HiveException | IOException e) {
      if (this.sessionState != null) {
        try {
          this.sessionState.close();
        } catch (IOException ioException) {
          LOG.error("Error while closing SessionState", ioException);
        }
      }
      if (this.hiveDriver != null) {
        this.hiveDriver.close();
      }
      throw new HoodieHiveSyncException("Failed to create HiveMetaStoreClient", e);
    }

    try {
      this.partitionValueExtractor =
          (PartitionValueExtractor) Class.forName(cfg.partitionValueExtractorClass).newInstance();
    } catch (Exception e) {
      throw new HoodieHiveSyncException(
          "Failed to initialize PartitionValueExtractor class " + cfg.partitionValueExtractorClass, e);
    }

    activeTimeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
  }

  public HoodieTimeline getActiveTimeline() {
    return activeTimeline;
  }

  /**
   * Add the (NEW) partitions to the table.
   */
  @Override
  public void addPartitionsToTable(String tableName, List<String> partitionsToAdd) {
    if (partitionsToAdd.isEmpty()) {
      LOG.info("No partitions to add for " + tableName);
      return;
    }
    LOG.info("Adding partitions " + partitionsToAdd.size() + " to table " + tableName);
    String sql = constructAddPartitions(tableName, partitionsToAdd);
    updateHiveSQL(sql);
  }

  /**
   * Partition path has changed - update the path for te following partitions.
   */
  @Override
  public void updatePartitionsToTable(String tableName, List<String> changedPartitions) {
    if (changedPartitions.isEmpty()) {
      LOG.info("No partitions to change for " + tableName);
      return;
    }
    LOG.info("Changing partitions " + changedPartitions.size() + " on " + tableName);
    List<String> sqls = constructChangePartitions(tableName, changedPartitions);
    for (String sql : sqls) {
      updateHiveSQL(sql);
    }
  }

  /**
   * Update the table properties to the table.
   */
  @Override
  public void updateTableProperties(String tableName, Map<String, String> tableProperties) {
    if (tableProperties == null || tableProperties.isEmpty()) {
      return;
    }
    try {
      Table table = client.getTable(syncConfig.databaseName, tableName);
      for (Map.Entry<String, String> entry: tableProperties.entrySet()) {
        table.putToParameters(entry.getKey(), entry.getValue());
      }
      client.alter_table(syncConfig.databaseName, tableName, table);
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to update table properties for table: "
          + tableName, e);
    }
  }

  private String constructAddPartitions(String tableName, List<String> partitions) {
    StringBuilder alterSQL = new StringBuilder("ALTER TABLE ");
    alterSQL.append(HIVE_ESCAPE_CHARACTER).append(syncConfig.databaseName)
            .append(HIVE_ESCAPE_CHARACTER).append(".").append(HIVE_ESCAPE_CHARACTER)
            .append(tableName).append(HIVE_ESCAPE_CHARACTER).append(" ADD IF NOT EXISTS ");
    for (String partition : partitions) {
      String partitionClause = getPartitionClause(partition);
      String fullPartitionPath = FSUtils.getPartitionPath(syncConfig.basePath, partition).toString();
      alterSQL.append("  PARTITION (").append(partitionClause).append(") LOCATION '").append(fullPartitionPath)
          .append("' ");
    }
    return alterSQL.toString();
  }

  /**
   * Generate Hive Partition from partition values.
   *
   * @param partition Partition path
   * @return
   */
  private String getPartitionClause(String partition) {
    List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partition);
    ValidationUtils.checkArgument(syncConfig.partitionFields.size() == partitionValues.size(),
        "Partition key parts " + syncConfig.partitionFields + " does not match with partition values " + partitionValues
            + ". Check partition strategy. ");
    List<String> partBuilder = new ArrayList<>();
    for (int i = 0; i < syncConfig.partitionFields.size(); i++) {
      String partitionValue = partitionValues.get(i);
      // decode the partition before sync to hive to prevent multiple escapes of HIVE
      if (syncConfig.decodePartition) {
        // This is a decode operator for encode in KeyGenUtils#getRecordPartitionPath
        partitionValue = PartitionPathEncodeUtils.unescapePathName(partitionValue);
      }
      partBuilder.add("`" + syncConfig.partitionFields.get(i) + "`='" + partitionValue + "'");
    }
    return String.join(",", partBuilder);
  }

  private List<String> constructChangePartitions(String tableName, List<String> partitions) {
    List<String> changePartitions = new ArrayList<>();
    // Hive 2.x doesn't like db.table name for operations, hence we need to change to using the database first
    String useDatabase = "USE " + HIVE_ESCAPE_CHARACTER + syncConfig.databaseName + HIVE_ESCAPE_CHARACTER;
    changePartitions.add(useDatabase);
    String alterTable = "ALTER TABLE " + HIVE_ESCAPE_CHARACTER + tableName + HIVE_ESCAPE_CHARACTER;
    for (String partition : partitions) {
      String partitionClause = getPartitionClause(partition);
      Path partitionPath = FSUtils.getPartitionPath(syncConfig.basePath, partition);
      String partitionScheme = partitionPath.toUri().getScheme();
      String fullPartitionPath = StorageSchemes.HDFS.getScheme().equals(partitionScheme)
              ? FSUtils.getDFSFullPartitionPath(fs, partitionPath) : partitionPath.toString();
      String changePartition =
          alterTable + " PARTITION (" + partitionClause + ") SET LOCATION '" + fullPartitionPath + "'";
      changePartitions.add(changePartition);
    }
    return changePartitions;
  }

  /**
   * Iterate over the storage partitions and find if there are any new partitions that need to be added or updated.
   * Generate a list of PartitionEvent based on the changes required.
   */
  List<PartitionEvent> getPartitionEvents(List<Partition> tablePartitions, List<String> partitionStoragePartitions) {
    Map<String, String> paths = new HashMap<>();
    for (Partition tablePartition : tablePartitions) {
      List<String> hivePartitionValues = tablePartition.getValues();
      String fullTablePartitionPath =
          Path.getPathWithoutSchemeAndAuthority(new Path(tablePartition.getSd().getLocation())).toUri().getPath();
      paths.put(String.join(", ", hivePartitionValues), fullTablePartitionPath);
    }

    List<PartitionEvent> events = new ArrayList<>();
    for (String storagePartition : partitionStoragePartitions) {
      Path storagePartitionPath = FSUtils.getPartitionPath(syncConfig.basePath, storagePartition);
      String fullStoragePartitionPath = Path.getPathWithoutSchemeAndAuthority(storagePartitionPath).toUri().getPath();
      // Check if the partition values or if hdfs path is the same
      List<String> storagePartitionValues = partitionValueExtractor.extractPartitionValuesInPath(storagePartition);
      if (!storagePartitionValues.isEmpty()) {
        String storageValue = String.join(", ", storagePartitionValues);
        if (!paths.containsKey(storageValue)) {
          events.add(PartitionEvent.newPartitionAddEvent(storagePartition));
        } else if (!paths.get(storageValue).equals(fullStoragePartitionPath)) {
          events.add(PartitionEvent.newPartitionUpdateEvent(storagePartition));
        }
      }
    }
    return events;
  }

  /**
   * Scan table partitions.
   */
  public List<Partition> scanTablePartitions(String tableName) throws TException {
    return client.listPartitions(syncConfig.databaseName, tableName, (short) -1);
  }

  void updateTableDefinition(String tableName, MessageType newSchema) {
    try {
      String newSchemaStr = HiveSchemaUtil.generateSchemaString(newSchema, syncConfig.partitionFields, syncConfig.supportTimestamp);
      // Cascade clause should not be present for non-partitioned tables
      String cascadeClause = syncConfig.partitionFields.size() > 0 ? " cascade" : "";
      StringBuilder sqlBuilder = new StringBuilder("ALTER TABLE ").append(HIVE_ESCAPE_CHARACTER)
              .append(syncConfig.databaseName).append(HIVE_ESCAPE_CHARACTER).append(".")
              .append(HIVE_ESCAPE_CHARACTER).append(tableName)
              .append(HIVE_ESCAPE_CHARACTER).append(" REPLACE COLUMNS(")
              .append(newSchemaStr).append(" )").append(cascadeClause);
      LOG.info("Updating table definition with " + sqlBuilder);
      updateHiveSQL(sqlBuilder.toString());
    } catch (IOException e) {
      throw new HoodieHiveSyncException("Failed to update table for " + tableName, e);
    }
  }

  @Override
  public void createTable(String tableName, MessageType storageSchema, String inputFormatClass,
                          String outputFormatClass, String serdeClass,
                          Map<String, String> serdeProperties, Map<String, String> tableProperties) {
    try {
      String createSQLQuery =
          HiveSchemaUtil.generateCreateDDL(tableName, storageSchema, syncConfig, inputFormatClass,
              outputFormatClass, serdeClass, serdeProperties, tableProperties);
      LOG.info("Creating table with " + createSQLQuery);
      updateHiveSQL(createSQLQuery);
    } catch (IOException e) {
      throw new HoodieHiveSyncException("Failed to create table " + tableName, e);
    }
  }

  /**
   * Get the table schema.
   */
  @Override
  public Map<String, String> getTableSchema(String tableName) {
    if (syncConfig.useJdbc) {
      if (!doesTableExist(tableName)) {
        throw new IllegalArgumentException(
            "Failed to get schema for table " + tableName + " does not exist");
      }
      Map<String, String> schema = new HashMap<>();
      ResultSet result = null;
      try {
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        result = databaseMetaData.getColumns(null, syncConfig.databaseName, tableName, null);
        while (result.next()) {
          TYPE_CONVERTOR.doConvert(result, schema);
        }
        return schema;
      } catch (SQLException e) {
        throw new HoodieHiveSyncException("Failed to get table schema for " + tableName, e);
      } finally {
        closeQuietly(result, null);
      }
    } else {
      return getTableSchemaUsingMetastoreClient(tableName);
    }
  }

  public Map<String, String> getTableSchemaUsingMetastoreClient(String tableName) {
    try {
      // HiveMetastoreClient returns partition keys separate from Columns, hence get both and merge to
      // get the Schema of the table.
      final long start = System.currentTimeMillis();
      Table table = this.client.getTable(syncConfig.databaseName, tableName);
      Map<String, String> partitionKeysMap =
          table.getPartitionKeys().stream().collect(Collectors.toMap(FieldSchema::getName, f -> f.getType().toUpperCase()));

      Map<String, String> columnsMap =
          table.getSd().getCols().stream().collect(Collectors.toMap(FieldSchema::getName, f -> f.getType().toUpperCase()));

      Map<String, String> schema = new HashMap<>();
      schema.putAll(columnsMap);
      schema.putAll(partitionKeysMap);
      final long end = System.currentTimeMillis();
      LOG.info(String.format("Time taken to getTableSchema: %s ms", (end - start)));
      return schema;
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get table schema for : " + tableName, e);
    }
  }

  /**
   * @return true if the configured table exists
   */
  @Override
  public boolean doesTableExist(String tableName) {
    try {
      return client.tableExists(syncConfig.databaseName, tableName);
    } catch (TException e) {
      throw new HoodieHiveSyncException("Failed to check if table exists " + tableName, e);
    }
  }

  /**
   * @param databaseName
   * @return  true if the configured database exists
   */
  public boolean doesDataBaseExist(String databaseName) {
    try {
      Database database = client.getDatabase(databaseName);
      if (database != null && databaseName.equals(database.getName())) {
        return true;
      }
    } catch (TException e) {
      throw new HoodieHiveSyncException("Failed to check if database exists " + databaseName, e);
    }
    return false;
  }

  /**
   * Execute a update in hive metastore with this SQL.
   *
   * @param s SQL to execute
   */
  public void updateHiveSQL(String s) {
    if (syncConfig.useJdbc) {
      Statement stmt = null;
      try {
        stmt = connection.createStatement();
        LOG.info("Executing SQL " + s);
        stmt.execute(s);
      } catch (SQLException e) {
        throw new HoodieHiveSyncException("Failed in executing SQL " + s, e);
      } finally {
        closeQuietly(null, stmt);
      }
    } else {
      updateHiveSQLUsingHiveDriver(s);
    }
  }

  /**
   * Execute a update in hive using Hive Driver.
   *
   * @param sql SQL statement to execute
   */
  public CommandProcessorResponse updateHiveSQLUsingHiveDriver(String sql) {
    List<CommandProcessorResponse> responses = updateHiveSQLs(Collections.singletonList(sql));
    return responses.get(responses.size() - 1);
  }

  private List<CommandProcessorResponse> updateHiveSQLs(List<String> sqls) {
    List<CommandProcessorResponse> responses = new ArrayList<>();
    try {
      for (String sql : sqls) {
        if (hiveDriver != null) {
          final long start = System.currentTimeMillis();
          responses.add(hiveDriver.run(sql));
          final long end = System.currentTimeMillis();
          LOG.info(String.format("Time taken to execute [%s]: %s ms", sql, (end - start)));
        }
      }
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed in executing SQL", e);
    }
    return responses;
  }

  private void createHiveConnection() {
    if (connection == null) {
      try {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
      } catch (ClassNotFoundException e) {
        LOG.error("Unable to load Hive driver class", e);
        return;
      }

      try {
        this.connection = DriverManager.getConnection(syncConfig.jdbcUrl, syncConfig.hiveUser, syncConfig.hivePass);
        LOG.info("Successfully established Hive connection to  " + syncConfig.jdbcUrl);
      } catch (SQLException e) {
        throw new HoodieHiveSyncException("Cannot create hive connection " + getHiveJdbcUrlWithDefaultDBName(), e);
      }
    }
  }

  private String getHiveJdbcUrlWithDefaultDBName() {
    String hiveJdbcUrl = syncConfig.jdbcUrl;
    String urlAppend = null;
    // If the hive url contains addition properties like ;transportMode=http;httpPath=hs2
    if (hiveJdbcUrl.contains(";")) {
      urlAppend = hiveJdbcUrl.substring(hiveJdbcUrl.indexOf(";"));
      hiveJdbcUrl = hiveJdbcUrl.substring(0, hiveJdbcUrl.indexOf(";"));
    }
    if (!hiveJdbcUrl.endsWith("/")) {
      hiveJdbcUrl = hiveJdbcUrl + "/";
    }
    return hiveJdbcUrl + (urlAppend == null ? "" : urlAppend);
  }

  @Override
  public Option<String> getLastCommitTimeSynced(String tableName) {
    // Get the last commit time from the TBLproperties
    try {
      Table database = client.getTable(syncConfig.databaseName, tableName);
      return Option.ofNullable(database.getParameters().getOrDefault(HOODIE_LAST_COMMIT_TIME_SYNC, null));
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get the last commit time synced from the database", e);
    }
  }

  public void close() {
    try {
      if (connection != null) {
        connection.close();
      }
      if (client != null) {
        Hive.closeCurrent();
        client = null;
      }
    } catch (SQLException e) {
      LOG.error("Could not close connection ", e);
    }
  }

  List<String> getAllTables(String db) throws Exception {
    return client.getAllTables(db);
  }

  @Override
  public void updateLastCommitTimeSynced(String tableName) {
    // Set the last commit time from the TBLproperties
    String lastCommitSynced = activeTimeline.lastInstant().get().getTimestamp();
    try {
      Table table = client.getTable(syncConfig.databaseName, tableName);
      table.putToParameters(HOODIE_LAST_COMMIT_TIME_SYNC, lastCommitSynced);
      client.alter_table(syncConfig.databaseName, tableName, table);
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get update last commit time synced to " + lastCommitSynced, e);
    }
  }
}