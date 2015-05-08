/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.job.entries.sqoop;

import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.ArgumentWrapper;
import org.pentaho.di.job.CommandLineArgument;
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
  public static final String DELETE_TARGET_DIR = "deletTargetDir";

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

  public String getTargetDir() {
    return targetDir;
  }

  public void setTargetDir( String targetDir ) {
    String old = this.targetDir;
    this.targetDir = targetDir;
    pcs.firePropertyChange( TARGET_DIR, old, this.targetDir );
  }

  public String getWarehouseDir() {
    return warehouseDir;
  }

  public void setWarehouseDir( String warehouseDir ) {
    String old = this.warehouseDir;
    this.warehouseDir = warehouseDir;
    pcs.firePropertyChange( WAREHOUSE_DIR, old, this.warehouseDir );
  }

  public String getAppend() {
    return append;
  }

  public void setAppend( String append ) {
    String old = this.append;
    this.append = append;
    pcs.firePropertyChange( APPEND, old, this.append );
  }

  public String getAsAvrodatafile() {
    return asAvrodatafile;
  }

  public void setAsAvrodatafile( String asAvrodatafile ) {
    String old = this.asAvrodatafile;
    this.asAvrodatafile = asAvrodatafile;
    pcs.firePropertyChange( AS_AVRODATAFILE, old, this.asAvrodatafile );
  }

  public String getAsSequencefile() {
    return asSequencefile;
  }

  public void setAsSequencefile( String asSequencefile ) {
    String old = this.asSequencefile;
    this.asSequencefile = asSequencefile;
    pcs.firePropertyChange( AS_SEQUENCEFILE, old, this.asSequencefile );
  }

  public String getAsTextfile() {
    return asTextfile;
  }

  public void setAsTextfile( String asTextfile ) {
    String old = this.asTextfile;
    this.asTextfile = asTextfile;
    pcs.firePropertyChange( AS_TEXTFILE, old, this.asTextfile );
  }

  public String getBoundaryQuery() {
    return boundaryQuery;
  }

  public void setBoundaryQuery( String boundaryQuery ) {
    String old = this.boundaryQuery;
    this.boundaryQuery = boundaryQuery;
    pcs.firePropertyChange( BOUNDARY_QUERY, old, this.boundaryQuery );
  }

  public String getColumns() {
    return columns;
  }

  public void setColumns( String columns ) {
    String old = this.columns;
    this.columns = columns;
    pcs.firePropertyChange( COLUMNS, old, this.columns );
  }

  public String getDirect() {
    return direct;
  }

  public void setDirect( String direct ) {
    String old = this.direct;
    this.direct = direct;
    pcs.firePropertyChange( DIRECT, old, this.direct );
  }

  public String getDirectSplitSize() {
    return directSplitSize;
  }

  public void setDirectSplitSize( String directSplitSize ) {
    String old = this.directSplitSize;
    this.directSplitSize = directSplitSize;
    pcs.firePropertyChange( DIRECT_SPLIT_SIZE, old, this.directSplitSize );
  }

  public String getInlineLobLimit() {
    return inlineLobLimit;
  }

  public void setInlineLobLimit( String inlineLobLimit ) {
    String old = this.inlineLobLimit;
    this.inlineLobLimit = inlineLobLimit;
    pcs.firePropertyChange( INLINE_LOB_LIMIT, old, this.inlineLobLimit );
  }

  public String getSplitBy() {
    return splitBy;
  }

  public void setSplitBy( String splitBy ) {
    String old = this.splitBy;
    this.splitBy = splitBy;
    pcs.firePropertyChange( SPLIT_BY, old, this.splitBy );
  }

  public String getQuery() {
    return query;
  }

  public void setQuery( String query ) {
    String old = this.query;
    this.query = query;
    pcs.firePropertyChange( QUERY, old, this.query );
  }

  public String getWhere() {
    return where;
  }

  public void setWhere( String where ) {
    String old = this.where;
    this.where = where;
    pcs.firePropertyChange( WHERE, old, this.where );
  }

  public String getCompress() {
    return compress;
  }

  public void setCompress( String compress ) {
    String old = this.compress;
    this.compress = compress;
    pcs.firePropertyChange( COMPRESS, old, this.compress );
  }

  public String getCompressionCodec() {
    return compressionCodec;
  }

  public void setCompressionCodec( String compressionCodec ) {
    String old = this.compressionCodec;
    this.compressionCodec = compressionCodec;
    pcs.firePropertyChange( COMPRESSION_CODEC, old, this.compressionCodec );
  }

  public String getCheckColumn() {
    return checkColumn;
  }

  public void setCheckColumn( String checkColumn ) {
    String old = this.checkColumn;
    this.checkColumn = checkColumn;
    pcs.firePropertyChange( CHECK_COLUMN, old, this.checkColumn );
  }

  public String getIncremental() {
    return incremental;
  }

  public void setIncremental( String incremental ) {
    String old = this.incremental;
    this.incremental = incremental;
    pcs.firePropertyChange( INCREMENTAL, old, this.incremental );
  }

  public String getLastValue() {
    return lastValue;
  }

  public void setLastValue( String lastValue ) {
    String old = this.lastValue;
    this.lastValue = lastValue;
    pcs.firePropertyChange( LAST_VALUE, old, this.lastValue );
  }

  public String getHiveImport() {
    return hiveImport;
  }

  public void setHiveImport( String hiveImport ) {
    String old = this.hiveImport;
    this.hiveImport = hiveImport;
    pcs.firePropertyChange( HIVE_IMPORT, old, this.hiveImport );
  }

  public String getHiveOverwrite() {
    return hiveOverwrite;
  }

  public void setHiveOverwrite( String hiveOverwrite ) {
    String old = this.hiveOverwrite;
    this.hiveOverwrite = hiveOverwrite;
    pcs.firePropertyChange( HIVE_OVERWRITE, old, this.hiveOverwrite );
  }

  public String getCreateHiveTable() {
    return createHiveTable;
  }

  public void setCreateHiveTable( String createHiveTable ) {
    String old = this.createHiveTable;
    this.createHiveTable = createHiveTable;
    pcs.firePropertyChange( CREATE_HIVE_TABLE, old, this.createHiveTable );
  }

  public String getHiveTable() {
    return hiveTable;
  }

  public void setHiveTable( String hiveTable ) {
    String old = this.hiveTable;
    this.hiveTable = hiveTable;
    pcs.firePropertyChange( HIVE_TABLE, old, this.hiveTable );
  }

  public String getHiveDropImportDelims() {
    return hiveDropImportDelims;
  }

  public void setHiveDropImportDelims( String hiveDropImportDelims ) {
    String old = this.hiveDropImportDelims;
    this.hiveDropImportDelims = hiveDropImportDelims;
    pcs.firePropertyChange( HIVE_DROP_IMPORT_DELIMS, old, this.hiveDropImportDelims );
  }

  public String getHiveDelimsReplacement() {
    return hiveDelimsReplacement;
  }

  public void setHiveDelimsReplacement( String hiveDelimsReplacement ) {
    String old = this.hiveDelimsReplacement;
    this.hiveDelimsReplacement = hiveDelimsReplacement;
    pcs.firePropertyChange( HIVE_DELIMS_REPLACEMENT, old, this.hiveDelimsReplacement );
  }

  public String getColumnFamily() {
    return columnFamily;
  }

  public void setColumnFamily( String columnFamily ) {
    String old = this.columnFamily;
    this.columnFamily = columnFamily;
    pcs.firePropertyChange( COLUMN_FAMILY, old, this.columnFamily );
  }

  public String getHbaseCreateTable() {
    return hbaseCreateTable;
  }

  public void setHbaseCreateTable( String hbaseCreateTable ) {
    String old = this.hbaseCreateTable;
    this.hbaseCreateTable = hbaseCreateTable;
    pcs.firePropertyChange( HBASE_CREATE_TABLE, old, this.hbaseCreateTable );
  }

  public String getHbaseRowKey() {
    return hbaseRowKey;
  }

  public void setHbaseRowKey( String hbaseRowKey ) {
    String old = this.hbaseRowKey;
    this.hbaseRowKey = hbaseRowKey;
    pcs.firePropertyChange( HBASE_ROW_KEY, old, this.hbaseRowKey );
  }

  public String getHbaseTable() {
    return hbaseTable;
  }

  public void setHbaseTable( String hbaseTable ) {
    String old = this.hbaseTable;
    this.hbaseTable = hbaseTable;
    pcs.firePropertyChange( HBASE_TABLE, old, this.hbaseTable );
  }

  public String getHbaseZookeeperQuorum() {
    return hbaseZookeeperQuorum;
  }

  public void setHbaseZookeeperQuorum( String hbaseZookeeperQuorum ) {
    String old = this.hbaseZookeeperQuorum;
    this.hbaseZookeeperQuorum = hbaseZookeeperQuorum;
    pcs.firePropertyChange( HBASE_ZOOKEEPER_QUORUM, old, this.hbaseZookeeperQuorum );
  }

  public String getHbaseZookeeperClientPort() {
    return hbaseZookeeperClientPort;
  }

  public void setHbaseZookeeperClientPort( String hbaseZookeeperClientPort ) {
    String old = this.hbaseZookeeperClientPort;
    this.hbaseZookeeperClientPort = hbaseZookeeperClientPort;
    pcs.firePropertyChange( HBASE_ZOOKEEPER_CLIENT_PORT, old, this.hbaseZookeeperClientPort );
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
    String old = this.asParquetfile;
    this.asParquetfile = asParquetfile;
    pcs.firePropertyChange( AS_PARQUETFILE, old, this.asParquetfile );
  }

  public String getDeleteTargetDir() {
    return deleteTargetDir;
  }

  public void setDeleteTargetDir( String deleteTargetDir ) {
    String old = this.asParquetfile;
    this.deleteTargetDir = deleteTargetDir;
    pcs.firePropertyChange( AS_PARQUETFILE, old, this.asParquetfile );
  }

  public String getFetchSize() {
    return fetchSize;
  }

  public void setFetchSize( String fetchSize ) {
    String old = this.asParquetfile;
    this.fetchSize = fetchSize;
    pcs.firePropertyChange( AS_PARQUETFILE, old, this.asParquetfile );
  }

  public String getMergeKey() {
    return mergeKey;
  }

  public void setMergeKey( String mergeKey ) {
    String old = this.asParquetfile;
    this.mergeKey = mergeKey;
    pcs.firePropertyChange( AS_PARQUETFILE, old, this.asParquetfile );
  }

  public String getHiveDatabase() {
    return hiveDatabase;
  }

  public void setHiveDatabase( String hiveDatabase ) {
    String old = this.asParquetfile;
    this.hiveDatabase = hiveDatabase;
    pcs.firePropertyChange( AS_PARQUETFILE, old, this.asParquetfile );
  }

  public String getHbaseBulkload() {
    return hbaseBulkload;
  }

  public void setHbaseBulkload( String hbaseBulkload ) {
    String old = this.asParquetfile;
    this.hbaseBulkload = hbaseBulkload;
    pcs.firePropertyChange( AS_PARQUETFILE, old, this.asParquetfile );
  }

  public String getCreateHcatalogTable() {
    return createHcatalogTable;
  }

  public void setCreateHcatalogTable( String createHcatalogTable ) {
    String old = this.asParquetfile;
    this.createHcatalogTable = createHcatalogTable;
    pcs.firePropertyChange( AS_PARQUETFILE, old, this.asParquetfile );
  }

  public String getHcatalogStorageStanza() {
    return hcatalogStorageStanza;
  }

  public void setHcatalogStorageStanza( String hcatalogStorageStanza ) {
    String old = this.asParquetfile;
    this.hcatalogStorageStanza = hcatalogStorageStanza;
    pcs.firePropertyChange( AS_PARQUETFILE, old, this.asParquetfile );
  }

  public String getAccumuloBatchSize() {
    return accumuloBatchSize;
  }

  public void setAccumuloBatchSize( String accumuloBatchSize ) {
    String old = this.accumuloBatchSize;
    this.accumuloBatchSize = accumuloBatchSize;
    pcs.firePropertyChange( ACCUMULO_BATCH_SIZE, old, this.accumuloBatchSize );
  }

  public String getAccumuloColumnFamily() {
    return accumuloColumnFamily;
  }

  public void setAccumuloColumnFamily( String accumuloColumnFamily ) {
    String old = this.accumuloColumnFamily;
    this.accumuloColumnFamily = accumuloColumnFamily;
    pcs.firePropertyChange( ACCUMULO_COLUMN_FAMILY, old, this.accumuloColumnFamily );
  }

  public String getAccumuloCreateTable() {
    return accumuloCreateTable;
  }

  public void setAccumuloCreateTable( String accumuloCreateTable ) {
    String old = this.accumuloCreateTable;
    this.accumuloCreateTable = accumuloCreateTable;
    pcs.firePropertyChange( ACCUMULO_CREATE_TABLE, old, this.accumuloCreateTable );
  }

  public String getAccumuloInstance() {
    return accumuloInstance;
  }

  public void setAccumuloInstance( String accumuloInstance ) {
    String old = this.accumuloInstance;
    this.accumuloInstance = accumuloInstance;
    pcs.firePropertyChange( ACCUMULO_INSTANCE, old, this.accumuloInstance );
  }

  public String getAccumuloMaxLatency() {
    return accumuloMaxLatency;
  }

  public void setAccumuloMaxLatency( String accumuloMaxLatency ) {
    String old = this.accumuloMaxLatency;
    this.accumuloMaxLatency = accumuloMaxLatency;
    pcs.firePropertyChange( ACCUMULO_MAX_LATENCY, old, this.accumuloMaxLatency );
  }

  public String getAccumuloPassword() {
    return accumuloPassword;
  }

  public void setAccumuloPassword( String accumuloPassword ) {
    String old = this.accumuloPassword;
    this.accumuloPassword = accumuloPassword;
    pcs.firePropertyChange( ACCUMULO_PASSWORD, old, this.accumuloPassword );
  }

  public String getAccumuloRowKey() {
    return accumuloRowKey;
  }

  public void setAccumuloRowKey( String accumuloRowKey ) {
    String old = this.accumuloRowKey;
    this.accumuloRowKey = accumuloRowKey;
    pcs.firePropertyChange( ACCUMULO_ROW_KEY, old, this.accumuloRowKey );
  }

  public String getAccumuloTable() {
    return accumuloTable;
  }

  public void setAccumuloTable( String accumuloTable ) {
    String old = this.accumuloTable;
    this.accumuloTable = accumuloTable;
    pcs.firePropertyChange( ACCUMULO_TABLE, old, this.accumuloTable );
  }

  public String getAccumuloUser() {
    return accumuloUser;
  }

  public void setAccumuloUser( String accumuloUser ) {
    String old = this.accumuloUser;
    this.accumuloUser = accumuloUser;
    pcs.firePropertyChange( ACCUMULO_USER, old, this.accumuloUser );
  }

  public String getAccumuloVisibility() {
    return accumuloVisibility;
  }

  public void setAccumuloVisibility( String accumuloVisibility ) {
    String old = this.accumuloVisibility;
    this.accumuloVisibility = accumuloVisibility;
    pcs.firePropertyChange( ACCUMULO_VISIBILITY, old, this.accumuloVisibility );
  }

  public String getAccumuloZookeepers() {
    return accumuloZookeepers;
  }

  public void setAccumuloZookeepers( String accumuloZookeepers ) {
    String old = this.accumuloZookeepers;
    this.accumuloZookeepers = accumuloZookeepers;
    pcs.firePropertyChange( ACCUMULO_ZOOKEPERS, old, this.accumuloZookeepers );
  }

}
