/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.big.data.kettle.plugins.sqoop;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.ui.xul.util.AbstractModelList;

/**
 * Configuration for a Sqoop Import
 */
public class SqoopImportConfig extends SqoopConfig {
  // Import control arguments
  public static final String TARGET_DIR = "targetDir";
  public static final String WAREHOUSE_DIR = "warehouseDir";
  public static final String APPEND = "append";
  public static final String AS_AVRODATAFILE = "asAvrodatafile";
  public static final String AS_SEQUENCEFILE = "asSequencefile";
  public static final String AS_TEXTFILE = "asTextfile";
  public static final String BOUNDARY_QUERY = "boundaryQuery";
  public static final String COLUMNS = "columns";
  public static final String DIRECT = "direct";
  public static final String DIRECT_SPLIT_SIZE = "directSplitSize";
  public static final String INLINE_LOB_LIMIT = "inlineLobLimit";
  public static final String SPLIT_BY = "splitBy";
  public static final String QUERY = "query";
  public static final String WHERE = "where";
  public static final String COMPRESS = "compress";
  public static final String COMPRESSION_CODEC = "compressionCodec";

  // Incremental import arguments
  public static final String CHECK_COLUMN = "checkColumn";
  public static final String INCREMENTAL = "incremental";
  public static final String LAST_VALUE = "lastValue";

  // Hive arguments
  public static final String HIVE_IMPORT = "hiveImport";
  public static final String HIVE_OVERWRITE = "hiveOverwrite";
  public static final String CREATE_HIVE_TABLE = "createHiveTable";
  public static final String HIVE_TABLE = "hiveTable";
  public static final String HIVE_DROP_IMPORT_DELIMS = "hiveDropImportDelims";
  public static final String HIVE_DELIMS_REPLACEMENT = "hiveDelimsReplacement";

  // HBase arguments
  public static final String COLUMN_FAMILY = "columnFamily";
  public static final String HBASE_CREATE_TABLE = "hbaseCreateTable";
  public static final String HBASE_ROW_KEY = "hbaseRowKey";
  public static final String HBASE_TABLE = "hbaseTable";
  public static final String HBASE_ZOOKEEPER_QUORUM = "hbaseZookeeperQuorum";
  public static final String HBASE_ZOOKEEPER_CLIENT_PORT = "hbaseZookeeperClientPort";

  public static final String AS_PARQUETFILE = "asParquetfile";
  public static final String DELETE_TARGET_DIR = "deleteTargetDir";

  public static final String FETCH_SIZE = "fetchSize";
  public static final String MERGE_KEY = "mergeKey";

  public static final String HIVE_DATABASE = "hiveDatabase";
  public static final String HBASE_BULKLOADER = "hbaseBulkload";

  public static final String CREATE_HCATALOG_TABLE = "createHcatalogTable";
  public static final String HCATALOG_STORAGE_STANZA = "hcatalogStorageStanza";

  public static final String ACCUMULO_BATCH_SIZE = "accumuloBatchSize";
  public static final String ACCUMULO_COLUMN_FAMILY = "accumuloColumnFamily";
  public static final String ACCUMULO_CREATE_TABLE = "accumuloCreateTable";
  public static final String ACCUMULO_INSTANCE = "accumuloInstance";
  public static final String ACCUMULO_MAX_LATENCY = "accumuloMaxLatency";
  public static final String ACCUMULO_PASSWORD = "accumuloPassword";
  public static final String ACCUMULO_ROW_KEY = "accumuloRowKey";
  public static final String ACCUMULO_TABLE = "accumuloTable";
  public static final String ACCUMULO_USER = "accumuloUser";
  public static final String ACCUMULO_VISIBILITY = "accumuloVisibility";
  public static final String ACCUMULO_ZOOKEPERS = "accumuloZookeepers";
  private final SqoopImportJobEntry jobEntry;

  // Import control arguments
  @CommandLineArgument( name = "target-dir" )
  private String targetDir;
  @CommandLineArgument( name = "warehouse-dir" )
  private String warehouseDir;
  @CommandLineArgument( name = APPEND, flag = true )
  private String append;
  @CommandLineArgument( name = "as-avrodatafile", flag = true )
  private String asAvrodatafile;
  @CommandLineArgument( name = "as-sequencefile", flag = true )
  private String asSequencefile;
  @CommandLineArgument( name = "as-textfile", flag = true )
  private String asTextfile;
  @CommandLineArgument( name = "boundary-query" )
  private String boundaryQuery;
  @CommandLineArgument( name = COLUMNS )
  private String columns;
  @CommandLineArgument( name = DIRECT, flag = true )
  private String direct;
  @CommandLineArgument( name = "direct-split-size" )
  private String directSplitSize;
  @CommandLineArgument( name = "inline-lob-limit" )
  private String inlineLobLimit;
  @CommandLineArgument( name = "split-by" )
  private String splitBy;
  @CommandLineArgument( name = QUERY )
  private String query;
  @CommandLineArgument( name = WHERE )
  private String where;
  @CommandLineArgument( name = COMPRESS, flag = true )
  private String compress;
  @CommandLineArgument( name = "compression-codec" )
  private String compressionCodec;

  // Incremental import arguments
  @CommandLineArgument( name = "check-column" )
  private String checkColumn;
  @CommandLineArgument( name = INCREMENTAL )
  private String incremental;
  @CommandLineArgument( name = "last-value" )
  private String lastValue;

  // Hive arguments
  @CommandLineArgument( name = "hive-import", flag = true )
  private String hiveImport;
  @CommandLineArgument( name = "hive-overwrite", flag = true )
  private String hiveOverwrite;
  @CommandLineArgument( name = "create-hive-table", flag = true )
  private String createHiveTable;
  @CommandLineArgument( name = "hive-table" )
  private String hiveTable;
  @CommandLineArgument( name = "hive-drop-import-delims", flag = true )
  private String hiveDropImportDelims;
  @CommandLineArgument( name = "hive-delims-replacement" )
  private String hiveDelimsReplacement;

  // HBase arguments
  @CommandLineArgument( name = "column-family" )
  private String columnFamily;
  @CommandLineArgument( name = "hbase-create-table", flag = true )
  private String hbaseCreateTable;
  @CommandLineArgument( name = "hbase-row-key" )
  private String hbaseRowKey;
  @CommandLineArgument( name = "hbase-table" )
  private String hbaseTable;

  @CommandLineArgument( name = "as-parquetfile", flag = true )
  private String asParquetfile;
  @CommandLineArgument( name = "delete-target-dir", flag = true )
  private String deleteTargetDir;

  @CommandLineArgument( name = "fetch-size" )
  private String fetchSize;
  @CommandLineArgument( name = "merge-key" )
  private String mergeKey;

  @CommandLineArgument( name = "hive-database" )
  private String hiveDatabase;

  @CommandLineArgument( name = "hbase-bulkload", flag = true )
  private String hbaseBulkload;

  @CommandLineArgument( name = "create-hcatalog-table", flag = true )
  private String createHcatalogTable;
  @CommandLineArgument( name = "hcatalog-storage-stanza" )
  private String hcatalogStorageStanza;

  @CommandLineArgument( name = "accumulo-batch-size" )
  private String accumuloBatchSize;
  @CommandLineArgument( name = "accumulo-column-family" )
  private String accumuloColumnFamily;
  @CommandLineArgument( name = "accumulo-create-table", flag = true )
  private String accumuloCreateTable;
  @CommandLineArgument( name = "accumulo-instance" )
  private String accumuloInstance;
  @CommandLineArgument( name = "accumulo-max-latency" )
  private String accumuloMaxLatency;
  @CommandLineArgument( name = "accumulo-password" )
  private String accumuloPassword;
  @CommandLineArgument( name = "accumulo-row-key" )
  private String accumuloRowKey;
  @CommandLineArgument( name = "accumulo-table" )
  private String accumuloTable;
  @CommandLineArgument( name = "accumulo-user" )
  private String accumuloUser;
  @CommandLineArgument( name = "accumulo-visibility" )
  private String accumuloVisibility;
  @CommandLineArgument( name = "accumulo-zookeepers" )
  private String accumuloZookeepers;

  @CommandLineArgument( name = "input-null-non-string" )
  private String inputNullNonString;
  @CommandLineArgument( name = "input-null-string" )
  private String inputNullString;

  // Non command line arguments for configuring HBase connection information
  private String hbaseZookeeperQuorum;
  private String hbaseZookeeperClientPort;

  public SqoopImportConfig( SqoopImportJobEntry jobEntry ) {
    this.jobEntry = jobEntry;
  }

  @Override protected NamedCluster createClusterTemplate() {
    return jobEntry.getNamedClusterService().getClusterTemplate();
  }

  public String getTargetDir() {
    return targetDir;
  }

  public void setTargetDir( String targetDir ) {
    this.targetDir = propertyChange( TARGET_DIR, this.targetDir, targetDir );
  }

  public String getWarehouseDir() {
    return warehouseDir;
  }

  public void setWarehouseDir( String warehouseDir ) {
    this.warehouseDir = propertyChange( WAREHOUSE_DIR, this.warehouseDir, warehouseDir );
  }

  public String getAppend() {
    return append;
  }

  public void setAppend( String append ) {
    this.append = propertyChange( APPEND, this.append, append );
  }

  public String getAsAvrodatafile() {
    return asAvrodatafile;
  }

  public void setAsAvrodatafile( String asAvrodatafile ) {
    this.asAvrodatafile = propertyChange( AS_AVRODATAFILE, this.asAvrodatafile, asAvrodatafile );
  }

  public String getAsSequencefile() {
    return asSequencefile;
  }

  public void setAsSequencefile( String asSequencefile ) {
    this.asSequencefile = propertyChange( AS_SEQUENCEFILE, this.asSequencefile, asSequencefile );
  }

  public String getAsTextfile() {
    return asTextfile;
  }

  public void setAsTextfile( String asTextfile ) {
    this.asTextfile = propertyChange( AS_TEXTFILE, this.asTextfile, asTextfile );
  }

  public String getBoundaryQuery() {
    return boundaryQuery;
  }

  public void setBoundaryQuery( String boundaryQuery ) {
    this.boundaryQuery = propertyChange( BOUNDARY_QUERY, this.boundaryQuery, boundaryQuery );
  }

  public String getColumns() {
    return columns;
  }

  public void setColumns( String columns ) {
    this.columns = propertyChange( COLUMNS, this.columns, columns );
  }

  public String getDirect() {
    return direct;
  }

  public void setDirect( String direct ) {
    this.direct = propertyChange( DIRECT, this.direct, direct );
  }

  public String getDirectSplitSize() {
    return directSplitSize;
  }

  public void setDirectSplitSize( String directSplitSize ) {
    this.directSplitSize = propertyChange( DIRECT_SPLIT_SIZE, this.directSplitSize, directSplitSize );
  }

  public String getInlineLobLimit() {
    return inlineLobLimit;
  }

  public void setInlineLobLimit( String inlineLobLimit ) {
    this.inlineLobLimit = propertyChange( INLINE_LOB_LIMIT, this.inlineLobLimit, inlineLobLimit );
  }

  public String getSplitBy() {
    return splitBy;
  }

  public void setSplitBy( String splitBy ) {
    this.splitBy = propertyChange( SPLIT_BY, this.splitBy, splitBy );
  }

  public String getQuery() {
    return query;
  }

  public void setQuery( String query ) {
    this.query = propertyChange( QUERY, this.query, query );
  }

  public String getWhere() {
    return where;
  }

  public void setWhere( String where ) {
    this.where = propertyChange( WHERE, this.where, where );
  }

  public String getCompress() {
    return compress;
  }

  public void setCompress( String compress ) {
    this.compress = propertyChange( COMPRESS, this.compress, compress );
  }

  public String getCompressionCodec() {
    return compressionCodec;
  }

  public void setCompressionCodec( String compressionCodec ) {
    this.compressionCodec = propertyChange( COMPRESSION_CODEC, this.compressionCodec, compressionCodec );
  }

  public String getCheckColumn() {
    return checkColumn;
  }

  public void setCheckColumn( String checkColumn ) {
    this.checkColumn = propertyChange( CHECK_COLUMN, this.checkColumn, checkColumn );
  }

  public String getIncremental() {
    return incremental;
  }

  public void setIncremental( String incremental ) {
    this.incremental = propertyChange( INCREMENTAL, this.incremental, incremental );
  }

  public String getLastValue() {
    return lastValue;
  }

  public void setLastValue( String lastValue ) {
    this.lastValue = propertyChange( LAST_VALUE, this.lastValue, lastValue );
  }

  public String getHiveImport() {
    return hiveImport;
  }

  public void setHiveImport( String hiveImport ) {
    this.hiveImport = propertyChange( HIVE_IMPORT, this.hiveImport, hiveImport );
  }

  public String getHiveOverwrite() {
    return hiveOverwrite;
  }

  public void setHiveOverwrite( String hiveOverwrite ) {
    this.hiveOverwrite = propertyChange( HIVE_OVERWRITE, this.hiveOverwrite, hiveOverwrite );
  }

  public String getCreateHiveTable() {
    return createHiveTable;
  }

  public void setCreateHiveTable( String createHiveTable ) {
    this.createHiveTable = propertyChange( CREATE_HIVE_TABLE, this.createHiveTable, createHiveTable );
  }

  public String getHiveTable() {
    return hiveTable;
  }

  public void setHiveTable( String hiveTable ) {
    this.hiveTable = propertyChange( HIVE_TABLE, this.hiveTable, hiveTable );
  }

  public String getHiveDropImportDelims() {
    return hiveDropImportDelims;
  }

  public void setHiveDropImportDelims( String hiveDropImportDelims ) {
    this.hiveDropImportDelims =
      propertyChange( HIVE_DROP_IMPORT_DELIMS, this.hiveDropImportDelims, hiveDropImportDelims );
  }

  public String getHiveDelimsReplacement() {
    return hiveDelimsReplacement;
  }

  public void setHiveDelimsReplacement( String hiveDelimsReplacement ) {
    this.hiveDelimsReplacement =
      propertyChange( HIVE_DELIMS_REPLACEMENT, this.hiveDelimsReplacement, hiveDelimsReplacement );
  }

  public String getColumnFamily() {
    return columnFamily;
  }

  public void setColumnFamily( String columnFamily ) {
    this.columnFamily = propertyChange( COLUMN_FAMILY, this.columnFamily, columnFamily );
  }

  public String getHbaseCreateTable() {
    return hbaseCreateTable;
  }

  public void setHbaseCreateTable( String hbaseCreateTable ) {
    this.hbaseCreateTable = propertyChange( HBASE_CREATE_TABLE, this.hbaseCreateTable, hbaseCreateTable );
  }

  public String getHbaseRowKey() {
    return hbaseRowKey;
  }

  public void setHbaseRowKey( String hbaseRowKey ) {
    this.hbaseRowKey = propertyChange( HBASE_ROW_KEY, this.hbaseRowKey, hbaseRowKey );
  }

  public String getHbaseTable() {
    return hbaseTable;
  }

  public void setHbaseTable( String hbaseTable ) {
    this.hbaseTable = propertyChange( HBASE_TABLE, this.hbaseTable, hbaseTable );
  }

  public String getHbaseZookeeperQuorum() {
    return hbaseZookeeperQuorum;
  }

  public void setHbaseZookeeperQuorum( String hbaseZookeeperQuorum ) {
    this.hbaseZookeeperQuorum =
      propertyChange( HBASE_ZOOKEEPER_QUORUM, this.hbaseZookeeperQuorum, hbaseZookeeperQuorum );
  }

  public String getHbaseZookeeperClientPort() {
    return hbaseZookeeperClientPort;
  }

  public void setHbaseZookeeperClientPort( String hbaseZookeeperClientPort ) {
    this.hbaseZookeeperClientPort =
      propertyChange( HBASE_ZOOKEEPER_CLIENT_PORT, this.hbaseZookeeperClientPort, hbaseZookeeperClientPort );
  }

  @Override
  public AbstractModelList<ArgumentWrapper> getAdvancedArgumentsList() {
    AbstractModelList<ArgumentWrapper> items = super.getAdvancedArgumentsList();

    // Simple O(N) list walk to find the last index of HBase properties so we can
    // group the zookeeper properties with them
    int index = items.size();
    int i = 0;
    for ( ; i < items.size(); i++ ) {
      if ( items.get( i ).getName().startsWith( "hbase" ) ) {
        index = i + 1; // Add after this guy
      }
    }

    try {
      items.add( index, new ArgumentWrapper( HBASE_ZOOKEEPER_QUORUM, BaseMessages.getString( getClass(),
          "HBaseZookeeperQuorum.Label" ),
          false, "", 0, this, getClass().getMethod( "getHbaseZookeeperQuorum" ), getClass()
            .getMethod( "setHbaseZookeeperQuorum", String.class ) ) );
      items.add( index + 1, new ArgumentWrapper( HBASE_ZOOKEEPER_CLIENT_PORT, BaseMessages.getString( getClass(),
          "HBaseZookeeperClientPort.Label" ),
          false, "", 0, this, getClass().getMethod( "getHbaseZookeeperClientPort" ),
          getClass().getMethod( "setHbaseZookeeperClientPort", String.class ) ) );
    } catch ( NoSuchMethodException ex ) {
      throw new RuntimeException( ex );
    }

    return items;
  }

  public String getAsParquetfile() {
    return asParquetfile;
  }

  public void setAsParquetfile( String asParquetfile ) {
    this.asParquetfile = propertyChange( AS_PARQUETFILE, this.asParquetfile, asParquetfile );
  }

  public String getDeleteTargetDir() {
    return deleteTargetDir;
  }

  public void setDeleteTargetDir( String deleteTargetDir ) {
    this.deleteTargetDir = propertyChange( DELETE_TARGET_DIR, this.deleteTargetDir, deleteTargetDir );
  }

  public String getFetchSize() {
    return fetchSize;
  }

  public void setFetchSize( String fetchSize ) {
    this.fetchSize = propertyChange( FETCH_SIZE, this.fetchSize, fetchSize );
  }

  public String getMergeKey() {
    return mergeKey;
  }

  public void setMergeKey( String mergeKey ) {
    this.mergeKey = propertyChange( MERGE_KEY, this.mergeKey, mergeKey );
  }

  public String getHiveDatabase() {
    return hiveDatabase;
  }

  public void setHiveDatabase( String hiveDatabase ) {
    this.hiveDatabase = propertyChange( HIVE_DATABASE, this.hiveDatabase, hiveDatabase );
  }

  public String getHbaseBulkload() {
    return hbaseBulkload;
  }

  public void setHbaseBulkload( String hbaseBulkload ) {
    this.hbaseBulkload = propertyChange( HBASE_BULKLOADER, this.hbaseBulkload, hbaseBulkload );
  }

  public String getCreateHcatalogTable() {
    return createHcatalogTable;
  }

  public void setCreateHcatalogTable( String createHcatalogTable ) {
    this.createHcatalogTable = propertyChange( CREATE_HCATALOG_TABLE, this.createHcatalogTable, createHcatalogTable );
  }

  public String getHcatalogStorageStanza() {
    return hcatalogStorageStanza;
  }

  public void setHcatalogStorageStanza( String hcatalogStorageStanza ) {
    this.hcatalogStorageStanza =
      propertyChange( HCATALOG_STORAGE_STANZA, this.hcatalogStorageStanza, hcatalogStorageStanza );
  }

  public String getAccumuloBatchSize() {
    return accumuloBatchSize;
  }

  public void setAccumuloBatchSize( String accumuloBatchSize ) {
    this.accumuloBatchSize = propertyChange( ACCUMULO_BATCH_SIZE, this.accumuloBatchSize, accumuloBatchSize );
  }

  public String getAccumuloColumnFamily() {
    return accumuloColumnFamily;
  }

  public void setAccumuloColumnFamily( String accumuloColumnFamily ) {
    this.accumuloColumnFamily =
      propertyChange( ACCUMULO_COLUMN_FAMILY, this.accumuloColumnFamily, accumuloColumnFamily );
  }

  public String getAccumuloCreateTable() {
    return accumuloCreateTable;
  }

  public void setAccumuloCreateTable( String accumuloCreateTable ) {
    this.accumuloCreateTable = propertyChange( ACCUMULO_CREATE_TABLE, this.accumuloCreateTable, accumuloCreateTable );
  }

  public String getAccumuloInstance() {
    return accumuloInstance;
  }

  public void setAccumuloInstance( String accumuloInstance ) {
    this.accumuloInstance = propertyChange( ACCUMULO_INSTANCE, this.accumuloInstance, accumuloInstance );
  }

  public String getAccumuloMaxLatency() {
    return accumuloMaxLatency;
  }

  public void setAccumuloMaxLatency( String accumuloMaxLatency ) {
    this.accumuloMaxLatency = propertyChange( ACCUMULO_MAX_LATENCY, this.accumuloMaxLatency, accumuloMaxLatency );
  }

  public String getAccumuloPassword() {
    return accumuloPassword;
  }

  public void setAccumuloPassword( String accumuloPassword ) {
    this.accumuloPassword = propertyChange( ACCUMULO_PASSWORD, this.accumuloPassword, accumuloPassword );
  }

  public String getAccumuloRowKey() {
    return accumuloRowKey;
  }

  public void setAccumuloRowKey( String accumuloRowKey ) {
    this.accumuloRowKey = propertyChange( ACCUMULO_ROW_KEY, this.accumuloRowKey, accumuloRowKey );
  }

  public String getAccumuloTable() {
    return accumuloTable;
  }

  public void setAccumuloTable( String accumuloTable ) {
    this.accumuloTable = propertyChange( ACCUMULO_TABLE, this.accumuloTable, accumuloTable );
  }

  public String getAccumuloUser() {
    return accumuloUser;
  }

  public void setAccumuloUser( String accumuloUser ) {
    this.accumuloUser = propertyChange( ACCUMULO_USER, this.accumuloUser, accumuloUser );
  }

  public String getAccumuloVisibility() {
    return accumuloVisibility;
  }

  public void setAccumuloVisibility( String accumuloVisibility ) {
    this.accumuloVisibility = propertyChange( ACCUMULO_VISIBILITY, this.accumuloVisibility, accumuloVisibility );
  }

  public String getAccumuloZookeepers() {
    return accumuloZookeepers;
  }

  public void setAccumuloZookeepers( String accumuloZookeepers ) {
    this.accumuloZookeepers = propertyChange( ACCUMULO_ZOOKEPERS, this.accumuloZookeepers, accumuloZookeepers );
  }

}
