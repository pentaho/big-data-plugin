package org.pentaho.hbase.shim;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Properties;

import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.steps.hbaseinput.ColumnFilter;
import org.pentaho.hbase.mapping.HBaseValueMeta;

public abstract class HBaseAdmin {

  // constant connection keys
  public static final String DEFAULTS_KEY = "hbase.default";
  public static final String SITE_KEY = "hbase.site";
  public static final String ZOOKEEPER_QUORUM_KEY = "hbase.zookeeper.quorum";
  public static final String ZOOKEEPER_PORT_KEY = "hbase.zookeeper.property.clientPort";

  // constant table creation option keys (commented out keys don't exist as
  // options for
  // HColumnDescriptor in 0.90.3
  // public static final String COL_DESCRIPTOR_MIN_VERSIONS_KEY =
  // "col.descriptor.minVersions";
  public static final String COL_DESCRIPTOR_MAX_VERSIONS_KEY = "col.descriptor.maxVersions";
  // public static final String COL_DESCRIPTOR_KEEP_DELETED_CELLS_KEY =
  // "col.descriptor.keepDeletedCells";
  public static final String COL_DESCRIPTOR_COMPRESSION_KEY = "col.descriptor.compression";
  // public static final String COL_DESCRIPTOR_ENCODE_ON_DISK_KEY =
  // "col.descriptor.encodeOnDisk";
  // public static final String COL_DESCRIPTOR_DATA_BLOCK_ENCODING_KEY =
  // "col.descriptor.dataBlockEncoding";
  public static final String COL_DESCRIPTOR_IN_MEMORY_KEY = "col.descriptor.inMemory";
  public static final String COL_DESCRIPTOR_BLOCK_CACHE_ENABLED_KEY = "col.descriptor.blockCacheEnabled";
  public static final String COL_DESCRIPTOR_BLOCK_SIZE_KEY = "col.descriptor.blockSize";
  public static final String COL_DESCRIPTOR_TIME_TO_LIVE_KEY = "col.desciptor.timeToLive";
  public static final String COL_DESCRIPTOR_BLOOM_FILTER_KEY = "col.descriptor.bloomFilter";
  public static final String COL_DESCRIPTOR_SCOPE_KEY = "col.descriptor.scope";

  // constant HTable writing keys
  public static final String HTABLE_WRITE_BUFFER_SIZE_KEY = "htable.writeBufferSize";
  public static final String HTABLE_DISABLE_WRITE_TO_WAL_KEY = "htable.disableWriteToWal";

  public abstract void configureConnection(Properties connProps,
      List<String> logMessages) throws Exception;

  public abstract boolean tableExists(String tableName) throws Exception;

  public abstract void disableTable(String tableName) throws Exception;

  public abstract void deleteTable(String tableName) throws Exception;

  public abstract void createTable(String tableName,
      List<String> colFamilyNames, Properties creationProps) throws Exception;

  public abstract void newSourceTable(String tableName) throws Exception;

  public abstract void newSourceTableScan(byte[] keyLowerBound,
      byte[] keyUpperBound) throws Exception;

  /**
   * Add a column filter to the list of filters that the scanner will apply to
   * rows server-side.
   * 
   * @param cf the column filter to add
   * @param columnMeta the meta data for the column used in the filter to add
   * @param matchAny true if the list of filters (if not created yet) should be
   *          "match one" (and false if it should be "match all")
   * @throws Exception if a problem occurs
   */
  public abstract void addColumnFilterToScan(ColumnFilter cf,
      HBaseValueMeta columnMeta, VariableSpace vars, boolean matchAny)
      throws Exception;

  public abstract void executeSourceTableScan() throws Exception;

  public abstract void closeSourceTable() throws Exception;

  public abstract void closeSourceResultSet() throws Exception;

  public abstract void newTargetTable(String tableName, Properties props)
      throws Exception;

  public abstract void closeTargetTable() throws Exception;

  protected static HBaseBytesUtil s_bytesUtil;

  /**
   * Static factory method for getting an HBaseAdmin implementation
   * 
   * @return a concrete implementation of the HBaseAdmin API
   * @throws Exception if a problem occurs
   */
  public static HBaseAdmin createHBaseAdmin() throws Exception {

    return new DefaultHBaseAdmin();
  }

  /**
   * Static factory method for getting a byte utility implementation
   * 
   * @return
   * @throws Exception
   */
  public static HBaseBytesUtil getBytesUtil() throws Exception {
    if (s_bytesUtil == null) {
      s_bytesUtil = new DefaultHBaseBytesUtil();
    }

    return s_bytesUtil;
  }

  /**
   * Utility method to covert a string to a URL object.
   * 
   * @param pathOrURL file or http URL as a string
   * @return a URL
   * @throws MalformedURLException if there is a problem with the URL.
   */
  public static URL stringToURL(String pathOrURL) throws MalformedURLException {
    URL result = null;

    if (isEmpty(pathOrURL)) {
      if (pathOrURL.toLowerCase().startsWith("http://")
          || pathOrURL.toLowerCase().startsWith("file://")) {
        result = new URL(pathOrURL);
      } else {
        String c = "file://" + pathOrURL;
        result = new URL(c);
      }
    }

    return result;
  }

  public static boolean isEmpty(String toCheck) {
    return (toCheck == null || toCheck.length() == 0);
  }
}
