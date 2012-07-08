/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2012 by Pentaho : http://www.pentaho.com
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

package org.pentaho.cassandra;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DynamicCompositeType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;

/**
 * Class encapsulating read-only schema information for a column family. Has
 * utility routines for converting between Cassandra meta data and Kettle meta
 * data, and for deserializing values.
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision$
 */
public class CassandraColumnMetaData {
  public static final String UTF8 = "UTF-8";

  /** Name of the column family this meta data refers to */
  protected String m_columnFamilyName; // can be used as the key name

  /** Type of the key */
  protected String m_keyValidator; // name of the class for key validation

  /** Type of the column names (used for sorting columns) */
  protected String m_columnComparator; // name of the class for sorting column
                                       // names

  /** m_columnComparator converted to Charset encoding string */
  protected String m_columnNameEncoding;

  /**
   * Default validator for the column family (table) - we can use this as the
   * type for any columns specified in a SELECT clause which *arent* in the meta
   * data
   */
  protected String m_defaultValidationClass;

  /** Map of column names/types */
  protected Map<String, String> m_columnMeta;

  /** Map of column names to indexed values (if any) */
  protected Map<String, HashSet<Object>> m_indexedVals;

  /** Holds the schema textual description */
  protected StringBuffer m_schemaDescription;

  /**
   * Constructor.
   * 
   * @param conn connection to cassandra
   * @param columnFamily the name of the column family to maintain meta data
   *          for.
   * @throws Exception if a problem occurs during connection or when fetching
   *           meta data
   */
  public CassandraColumnMetaData(CassandraConnection conn, String columnFamily)
      throws Exception {
    m_columnFamilyName = columnFamily;

    refresh(conn);
  }

  public String getDefaultValidationClass() {
    return m_defaultValidationClass;
  }

  /**
   * Refreshes the encapsulated meta data for the column family.
   * 
   * @param conn the connection to cassandra to use for refreshing the meta data
   * @throws Exception if a problem occurs during connection or when fetching
   *           meta data
   */
  public void refresh(CassandraConnection conn) throws Exception {

    m_schemaDescription = new StringBuffer();

    // column families
    KsDef keySpace = conn.describeKeyspace();
    List<CfDef> colFams = null;
    if (keySpace != null) {
      colFams = keySpace.getCf_defs();
    } else {
      throw new Exception("Unable to get meta data on keyspace '"
          + conn.m_keyspaceName + "'");
    }

    // look for the requested column family
    CfDef colDefs = null;
    for (CfDef fam : colFams) {
      String columnFamilyName = fam.getName(); // table name
      if (columnFamilyName.equals(m_columnFamilyName)) {
        m_schemaDescription.append("Column family: " + m_columnFamilyName);
        m_keyValidator = fam.getKey_validation_class(); // key type
        m_columnComparator = fam.getComparator_type(); // column names encoded
                                                       // as
        m_defaultValidationClass = fam.getDefault_validation_class(); // default
                                                                      // column
                                                                      // type
        m_schemaDescription.append("\n\tKey validator: " + m_keyValidator);

        m_schemaDescription.append("\n\tColumn comparator: "
            + m_columnComparator);

        m_schemaDescription.append("\n\tDefault column validator: "
            + m_defaultValidationClass);
        /*
         * m_schemaDescription.append("\n\tDefault column validator: " +
         * m_defaultValidationClass
         * .substring(m_defaultValidationClass.lastIndexOf(".")+1,
         * m_defaultValidationClass.length()));
         */

        // these seem to have disappeared between 0.8.6 and 1.0.0!
        /*
         * m_schemaDescription.append("\n\tMemtable operations: " +
         * fam.getMemtable_operations_in_millions());
         * m_schemaDescription.append("\n\tMemtable throughput: " +
         * fam.getMemtable_throughput_in_mb());
         * m_schemaDescription.append("\n\tMemtable flush after: " +
         * fam.getMemtable_flush_after_mins());
         */

        // these have disappeared between 1.0.8 and 1.1.0!!
        // m_schemaDescription.append("\n\tRows cached: " +
        // fam.getRow_cache_size());
        // m_schemaDescription.append("\n\tRow cache save period: " +
        // fam.getRow_cache_save_period_in_seconds());
        // m_schemaDescription.append("\n\tKeys cached: " +
        // fam.getKey_cache_size());
        // m_schemaDescription.append("\n\tKey cached save period: " +
        // fam.getKey_cache_save_period_in_seconds());
        m_schemaDescription.append("\n\tRead repair chance: "
            + fam.getRead_repair_chance());
        m_schemaDescription
            .append("\n\tGC grace: " + fam.getGc_grace_seconds());
        m_schemaDescription.append("\n\tMin compaction threshold: "
            + fam.getMin_compaction_threshold());
        m_schemaDescription.append("\n\tMax compaction threshold: "
            + fam.getMax_compaction_threshold());
        m_schemaDescription.append("\n\tReplicate on write: "
            + fam.replicate_on_write);
        // String rowCacheP = fam.getRow_cache_provider();

        m_schemaDescription.append("\n\n\tColumn metadata:");

        colDefs = fam;
        break;
      }
    }

    if (colDefs == null) {
      throw new Exception("Unable to find requested column family '"
          + m_columnFamilyName + "' in keyspace '" + conn.m_keyspaceName + "'");
    }

    m_columnNameEncoding = m_columnComparator;

    // set up our meta data map
    m_columnMeta = new TreeMap<String, String>();
    m_indexedVals = new HashMap<String, HashSet<Object>>();

    String comment = colDefs.getComment();
    if (comment != null && comment.length() > 0) {
      extractIndexedMeta(comment, m_indexedVals);
    }

    Iterator<ColumnDef> colMetaData = colDefs.getColumn_metadataIterator();
    if (colMetaData != null) {
      while (colMetaData.hasNext()) {
        ColumnDef currentDef = colMetaData.next();
        ByteBuffer b = ByteBuffer.wrap(currentDef.getName());

        String colName = getColumnValue(b, m_columnComparator).toString();

        String colType = currentDef.getValidation_class();
        m_columnMeta.put(colName, colType);

        m_schemaDescription.append("\n\tColumn name: " + colName);

        m_schemaDescription.append("\n\t\tColumn validator: " + colType);

        String indexName = currentDef.getIndex_name();
        if (!Const.isEmpty(indexName)) {
          m_schemaDescription.append("\n\t\tIndex name: "
              + currentDef.getIndex_name());
        }

        if (m_indexedVals.containsKey(colName)) {
          HashSet<Object> indexedVals = m_indexedVals.get(colName);

          m_schemaDescription.append("\n\t\tLegal values: {");
          int count = 0;
          for (Object val : indexedVals) {
            m_schemaDescription.append(val.toString());
            count++;
            if (count != indexedVals.size()) {
              m_schemaDescription.append(",");
            } else {
              m_schemaDescription.append("}");
            }
          }
        }
      }
    }
  }

  protected void extractIndexedMeta(String comment,
      Map<String, HashSet<Object>> indexedVals) {
    if (comment.indexOf("@@@") < 0) {
      return;
    }

    String meta = comment.substring(comment.indexOf("@@@"),
        comment.lastIndexOf("@@@"));
    meta = meta.replace("@@@", "");
    String[] fields = meta.split(";");

    for (String field : fields) {
      field = field.trim();
      String[] parts = field.split(":");

      if (parts.length != 2) {
        continue;
      }

      String fieldName = parts[0].trim();
      String valsS = parts[1];
      valsS = valsS.replace("{", "");
      valsS = valsS.replace("}", "");

      String[] vals = valsS.split(",");

      if (vals.length > 0) {
        HashSet<Object> valsSet = new HashSet<Object>();

        for (String aVal : vals) {
          valsSet.add(aVal.trim());
        }

        indexedVals.put(fieldName, valsSet);
      }
    }
    // }
  }

  /**
   * Static utility routine for checking for the existence of a column family
   * (table)
   * 
   * @param conn the connection to use
   * @param columnFamily the column family to check for
   * @return true if the supplied column family name exists in the keyspace
   * @throws Exception if a problem occurs
   */
  public static boolean columnFamilyExists(CassandraConnection conn,
      String columnFamily) throws Exception {

    boolean found = false;

    // column families
    KsDef keySpace = conn.describeKeyspace();
    List<CfDef> colFams = null;
    if (keySpace != null) {
      colFams = keySpace.getCf_defs();
    } else {
      throw new Exception("Unable to get meta data on keyspace '"
          + conn.m_keyspaceName + "'");
    }

    // look for the requested column family
    for (CfDef fam : colFams) {
      String columnFamilyName = fam.getName(); // table name
      if (columnFamilyName.equals(columnFamily)) {
        found = true;
        break;
      }
    }

    return found;
  }

  /**
   * Static utility routine that returns a list of column families that exist in
   * the keyspace encapsulated in the supplied connection
   * 
   * @param conn the connection to use
   * @return a list of column families (tables)
   * @throws Exception if a problem occurs
   */
  public static List<String> getColumnFamilyNames(CassandraConnection conn)
      throws Exception {

    KsDef keySpace = conn.describeKeyspace();
    List<CfDef> colFams = null;
    if (keySpace != null) {
      colFams = keySpace.getCf_defs();
    } else {
      throw new Exception("Unable to get meta data on keyspace '"
          + conn.m_keyspaceName + "'");
    }

    List<String> colFamNames = new ArrayList<String>();
    for (CfDef fam : colFams) {
      colFamNames.add(fam.getName());
    }

    return colFamNames;
  }

  /**
   * Return the schema overview information
   * 
   * @return the textual description of the schema
   */
  public String getSchemaDescription() {
    return m_schemaDescription.toString();
  }

  /**
   * Return the Cassandra column type (internal cassandra class name relative to
   * org.apache.cassandra.db.marshal) for the given Kettle column.
   * 
   * @param vm the ValueMetaInterface for the Kettle column
   * @return the corresponding internal cassandra type.
   */
  public static String getCassandraTypeForValueMeta(ValueMetaInterface vm) {
    switch (vm.getType()) {
    case ValueMetaInterface.TYPE_STRING:
      return "UTF8Type";
    case ValueMetaInterface.TYPE_BIGNUMBER:
      return "DecimalType";
    case ValueMetaInterface.TYPE_BOOLEAN:
      return "BooleanType";
    case ValueMetaInterface.TYPE_INTEGER:
      return "LongType";
    case ValueMetaInterface.TYPE_NUMBER:
      return "DoubleType";
    case ValueMetaInterface.TYPE_DATE:
      return "DateType";
    case ValueMetaInterface.TYPE_BINARY:
    case ValueMetaInterface.TYPE_SERIALIZABLE:
      return "BytesType";
    }

    return "UTF8Type";
  }

  /**
   * Return the Cassandra CQL column/key type for the given Kettle column. We
   * use this type for CQL create column family statements since, for some
   * reason, the internal type isn't recognized for the key. Internal types
   * *are* recognized for column definitions. The CQL reference guide states
   * that fully qualified (or relative to org.apache.cassandra.db.marshal) class
   * names can be used instead of CQL types - however, using these when defining
   * the key type always results in BytesType getting set for the key for some
   * reason.
   * 
   * @param vm the ValueMetaInterface for the Kettle column
   * @return the corresponding CQL type
   */
  public static String getCQLTypeForValueMeta(ValueMetaInterface vm) {
    switch (vm.getType()) {
    case ValueMetaInterface.TYPE_STRING:
      return "varchar";
    case ValueMetaInterface.TYPE_BIGNUMBER:
      return "decimal";
    case ValueMetaInterface.TYPE_BOOLEAN:
      return "boolean";
    case ValueMetaInterface.TYPE_INTEGER:
      return "bigint";
    case ValueMetaInterface.TYPE_NUMBER:
      return "double";
    case ValueMetaInterface.TYPE_DATE:
      return "timestamp";
    case ValueMetaInterface.TYPE_BINARY:
    case ValueMetaInterface.TYPE_SERIALIZABLE:
      return "blob";
    }

    return "blob";
  }

  /**
   * Static utility method that converts a Kettle value into an appropriately
   * encoded CQL string.
   * 
   * @param vm the ValueMeta for the Kettle value
   * @param value the actual Kettle value
   * @return an appropriately encoded CQL string representation of the value,
   *         suitable for using in an CQL query.
   * @throws KettleValueException if there is an error converting.
   */
  public static String kettleValueToCQL(ValueMetaInterface vm, Object value)
      throws KettleValueException {

    switch (vm.getType()) {
    case ValueMetaInterface.TYPE_STRING: {
      UTF8Type u = UTF8Type.instance;
      String toConvert = vm.getString(value);
      ByteBuffer decomposed = u.decompose(toConvert);
      String cassandraString = u.getString(decomposed);
      return escapeSingleQuotes(cassandraString);
    }
    case ValueMetaInterface.TYPE_BIGNUMBER: {
      DecimalType dt = DecimalType.instance;
      BigDecimal toConvert = vm.getBigNumber(value);
      ByteBuffer decomposed = dt.decompose(toConvert);
      String cassandraString = dt.getString(decomposed);
      return cassandraString;
    }
    case ValueMetaInterface.TYPE_BOOLEAN: {
      BooleanType bt = BooleanType.instance;
      Boolean toConvert = vm.getBoolean(value);
      ByteBuffer decomposed = bt.decompose(toConvert);
      String cassandraString = bt.getString(decomposed);
      return escapeSingleQuotes(cassandraString);
    }
    case ValueMetaInterface.TYPE_INTEGER: {
      LongType lt = LongType.instance;
      Long toConvert = vm.getInteger(value);
      ByteBuffer decomposed = lt.decompose(toConvert);
      String cassandraString = lt.getString(decomposed);
      return cassandraString;
    }
    case ValueMetaInterface.TYPE_NUMBER: {
      DoubleType dt = DoubleType.instance;
      Double toConvert = vm.getNumber(value);
      ByteBuffer decomposed = dt.decompose(toConvert);
      String cassandraString = dt.getString(decomposed);
      return cassandraString;
    }
    case ValueMetaInterface.TYPE_DATE:
      DateType d = DateType.instance;
      Date toConvert = vm.getDate(value);
      ByteBuffer decomposed = d.decompose(toConvert);
      String cassandraFormattedDateString = d.getString(decomposed);
      return escapeSingleQuotes(cassandraFormattedDateString);
    case ValueMetaInterface.TYPE_BINARY:
    case ValueMetaInterface.TYPE_SERIALIZABLE:
      throw new KettleValueException("Can't convert binary/serializable data "
          + "to CQL-compatible values"); // What to do here??? TODO
    }

    throw new KettleValueException("Not sure how to encode " + vm.toString()
        + " to" + " CQL-compatible values");
  }

  protected static String escapeSingleQuotes(String source) {

    // escaped by doubling (as in SQL)
    return source.replace("'", "''");
  }

  /**
   * Encode a string representation of a column name using the serializer for
   * the default comparator.
   * 
   * @param colName the textual column name to serialze
   * @return a ByteBuffer encapsulating the serialized column name
   * @throws KettleException if a problem occurs during serialization
   */
  public ByteBuffer columnNameToByteBuffer(String colName)
      throws KettleException {
    // TODO

    AbstractType serializer = null;
    String fullEncoder = m_columnComparator;
    String encoder = fullEncoder;

    // if it's a composite type make sure that we check only against the
    // primary type
    if (encoder.indexOf('(') > 0) {
      encoder = encoder.substring(0, encoder.indexOf('('));
    }

    if (encoder.indexOf("UTF8Type") > 0) {
      serializer = UTF8Type.instance;
    } else if (encoder.indexOf("AsciiType") > 0) {
      serializer = AsciiType.instance;
    } else if (encoder.indexOf("LongType") > 0) {
      serializer = LongType.instance;
    } else if (encoder.indexOf("DoubleType") > 0) {
      serializer = DoubleType.instance;
    } else if (encoder.indexOf("DateType") > 0) {
      serializer = DateType.instance;
    } else if (encoder.indexOf("IntegerType") > 0) {
      serializer = IntegerType.instance;
    } else if (encoder.indexOf("FloatType") > 0) {
      serializer = FloatType.instance;
    } else if (encoder.indexOf("LexicalUUIDType") > 0) {
      serializer = LexicalUUIDType.instance;
    } else if (encoder.indexOf("UUIDType") > 0) {
      serializer = UUIDType.instance;
    } else if (encoder.indexOf("BooleanType") > 0) {
      serializer = BooleanType.instance;
    } else if (encoder.indexOf("Int32Type") > 0) {
      serializer = Int32Type.instance;
    } else if (encoder.indexOf("DecimalType") > 0) {
      serializer = DecimalType.instance;
    } else if (encoder.indexOf("DynamicCompositeType") > 0) {
      try {
        serializer = TypeParser.parse(fullEncoder);
      } catch (ConfigurationException e) {
        throw new KettleException(e.getMessage(), e);
      }
    } else if (encoder.indexOf("CompositeType") > 0) {
      try {
        serializer = TypeParser.parse(fullEncoder);
      } catch (ConfigurationException e) {
        throw new KettleException(e.getMessage(), e);
      }
    }

    ByteBuffer result = serializer.fromString(colName);

    return result;
  }

  /**
   * Encodes and object via serialization
   * 
   * @param obj the object to encode
   * @return an array of bytes containing the serialized object
   * @throws IOException if serialization fails
   * 
   *           public static byte[] encodeObject(Object obj) throws IOException
   *           { ByteArrayOutputStream bos = new ByteArrayOutputStream();
   *           BufferedOutputStream buf = new BufferedOutputStream(bos);
   *           ObjectOutputStream oos = new ObjectOutputStream(buf);
   *           oos.writeObject(obj); buf.flush();
   * 
   *           return bos.toByteArray(); }
   */

  /**
   * Get the Kettle ValueMeta the corresponds to the type of the key for this
   * column family.
   * 
   * @return the key's ValueMeta
   */
  public ValueMetaInterface getValueMetaForKey() {
    return getValueMetaForColumn(getKeyName());
  }

  /**
   * Get the Kettle ValueMeta that corresponds to the type of the supplied
   * cassandra column.
   * 
   * @param colName the name of the column to get a ValueMeta for
   * @return the ValueMeta that is appropriate for the type of the supplied
   *         column.
   */
  public ValueMetaInterface getValueMetaForColumn(String colName) {
    String type = null;
    // check the key first
    if (colName.equals(getKeyName())) {
      type = m_keyValidator;
    } else {
      type = m_columnMeta.get(colName);
      if (type == null) {
        type = m_defaultValidationClass;
      }
    }

    int kettleType = 0;
    if (type.indexOf("UTF8Type") > 0 || type.indexOf("AsciiType") > 0
        || type.indexOf("UUIDType") > 0 || type.indexOf("CompositeType") > 0) {
      kettleType = ValueMetaInterface.TYPE_STRING;
    } else if (type.indexOf("LongType") > 0 || type.indexOf("IntegerType") > 0
        || type.indexOf("Int32Type") > 0) {
      kettleType = ValueMetaInterface.TYPE_INTEGER;
    } else if (type.indexOf("DoubleType") > 0 || type.indexOf("FloatType") > 0) {
      kettleType = ValueMetaInterface.TYPE_NUMBER;
    } else if (type.indexOf("DateType") > 0) {
      kettleType = ValueMetaInterface.TYPE_DATE;
    } else if (type.indexOf("DecimalType") > 0) {
      kettleType = ValueMetaInterface.TYPE_BIGNUMBER;
    } else if (type.indexOf("BytesType") > 0) {
      kettleType = ValueMetaInterface.TYPE_BINARY;
    } else if (type.indexOf("BooleanType") > 0) {
      kettleType = ValueMetaInterface.TYPE_BOOLEAN;
    }

    ValueMetaInterface newVM = new ValueMeta(colName, kettleType);
    if (m_indexedVals.containsKey(colName)) {
      // make it indexed!
      newVM.setStorageType(ValueMetaInterface.STORAGE_TYPE_INDEXED);
      HashSet<Object> indexedV = m_indexedVals.get(colName);
      Object[] iv = indexedV.toArray();
      newVM.setIndex(iv);
    }

    return newVM;
  }

  /**
   * Get a list of ValueMetas corresponding to the columns in this schema
   * 
   * @return a list of ValueMetas
   */
  public List<ValueMetaInterface> getValueMetasForSchema() {
    List<ValueMetaInterface> newL = new ArrayList<ValueMetaInterface>();

    for (String colName : m_columnMeta.keySet()) {
      ValueMetaInterface colVM = getValueMetaForColumn(colName);
      newL.add(colVM);
    }

    return newL;
  }

  /**
   * Get a Set of column names that are defined in the meta data for this schema
   * 
   * @return a set of column names.
   */
  public Set<String> getColumnNames() {
    // only returns those column names that are defined in the schema!
    return m_columnMeta.keySet();
  }

  /**
   * Returns true if the supplied column name exists in this schema.
   * 
   * @param colName the name of the column to check.
   * @return true if the column exists in the meta data for this column family.
   */
  public boolean columnExistsInSchema(String colName) {
    return (m_columnMeta.get(colName) != null);
  }

  /**
   * Get the name of the key for this column family (equals the name of the
   * column family).
   * 
   * @return the name of the key
   */
  public String getKeyName() {
    // we use the column family/table name as the key
    return getColumnFamilyName();
  }

  /**
   * Return the name of this column family.
   * 
   * @return the name of this column family.
   */
  public String getColumnFamilyName() {
    return m_columnFamilyName;
  }

  /**
   * Return the decoded key value of a row. Assumes that the supplied row comes
   * from the column family that this meta data represents!!
   * 
   * @param row a Cassandra row
   * @return the decoded key value
   * @throws KettleException if a deserializer can't be determined
   */
  public Object getKeyValue(CqlRow row) throws KettleException {
    /*
     * byte[] key = row.getKey();
     * 
     * return getColumnValue(key, m_keyValidator);
     */

    ByteBuffer key = row.bufferForKey();

    if (m_keyValidator.indexOf("BytesType") > 0) {
      return row.getKey();
    }

    return getColumnValue(key, m_keyValidator);
  }

  /**
   * Return the decoded key value of a row. Assumes that the supplied row comes
   * from the column family that this meta data represents!!
   * 
   * @param row a Cassandra row
   * @return the decoded key value
   * @throws KettleException if a deserializer can't be determined
   */
  public Object getKeyValue(KeySlice row) throws KettleException {
    ByteBuffer key = row.bufferForKey();

    if (m_keyValidator.indexOf("BytesType") > 0) {
      return row.getKey();
    }

    return getColumnValue(key, m_keyValidator);
  }

  public String getColumnName(Column aCol) throws KettleException {
    ByteBuffer b = aCol.bufferForName();

    String decodedColName = getColumnValue(b, m_columnComparator).toString();
    return decodedColName;
  }

  private Object getColumnValue(ByteBuffer valueBuff, String decoder)
      throws KettleException {
    if (valueBuff == null) {
      return null;
    }

    Object result = null;
    AbstractType deserializer = null;
    String fullDecoder = decoder;

    // if it's a composite type make sure that we check only against the
    // primary type
    if (decoder.indexOf('(') > 0) {
      decoder = decoder.substring(0, decoder.indexOf('('));
    }

    if (decoder.indexOf("UTF8Type") > 0) {
      deserializer = UTF8Type.instance;
    } else if (decoder.indexOf("AsciiType") > 0) {
      deserializer = AsciiType.instance;
    } else if (decoder.indexOf("LongType") > 0) {
      deserializer = LongType.instance;
    } else if (decoder.indexOf("DoubleType") > 0) {
      deserializer = DoubleType.instance;
    } else if (decoder.indexOf("DateType") > 0) {
      deserializer = DateType.instance;
    } else if (decoder.indexOf("IntegerType") > 0) {
      deserializer = IntegerType.instance;

      result = new Long(((IntegerType) deserializer).compose(valueBuff)
          .longValue());
      return result;
    } else if (decoder.indexOf("FloatType") > 0) {
      deserializer = FloatType.instance;

      result = new Double(((FloatType) deserializer).compose(valueBuff))
          .doubleValue();
      return result;
    } else if (decoder.indexOf("LexicalUUIDType") > 0) {
      deserializer = LexicalUUIDType.instance;

      result = new String(((LexicalUUIDType) deserializer).compose(valueBuff)
          .toString());
      return result;
    } else if (decoder.indexOf("UUIDType") > 0) {
      deserializer = UUIDType.instance;

      result = new String(((UUIDType) deserializer).compose(valueBuff)
          .toString());
      return result;
    } else if (decoder.indexOf("BooleanType") > 0) {
      deserializer = BooleanType.instance;
    } else if (decoder.indexOf("Int32Type") > 0) {
      deserializer = Int32Type.instance;

      result = new Long(((Int32Type) deserializer).compose(valueBuff))
          .longValue();
      return result;
    } else if (decoder.indexOf("DecimalType") > 0) {
      deserializer = DecimalType.instance;
    } else if (decoder.indexOf("DynamicCompositeType") > 0) {
      try {
        deserializer = TypeParser.parse(fullDecoder);

        // now return the string representation of the composite value
        result = ((DynamicCompositeType) deserializer).getString(valueBuff);
        return result;
      } catch (ConfigurationException e) {
        throw new KettleException(e.getMessage(), e);
      }
    } else if (decoder.indexOf("CompositeType") > 0) {
      try {
        deserializer = TypeParser.parse(fullDecoder);

        // now return the string representation of the composite value
        result = ((CompositeType) deserializer).getString(valueBuff);
        return result;
      } catch (ConfigurationException e) {
        throw new KettleException(e.getMessage(), e);
      }
    }

    if (deserializer == null) {
      throw new KettleException("Can't find deserializer for type '"
          + fullDecoder + "'");
    }

    result = deserializer.compose(valueBuff);

    return result;
  }

  /**
   * Decode the supplied column value. Uses the default validation class to
   * decode the value if the column is not explicitly defined in the schema.
   * 
   * @param aCol
   * @return
   * @throws KettleException
   */
  public Object getColumnValue(Column aCol) throws KettleException {
    String colName = getColumnName(aCol);

    // Clients should use getKey() for getting the key
    if (colName.equals("KEY")) {
      return null;
    }

    String decoder = m_columnMeta.get(colName);
    if (decoder == null) {
      // column is not in schema so use default validator
      decoder = m_defaultValidationClass;
    }

    String fullDecoder = decoder;
    if (decoder.indexOf('(') > 0) {
      decoder = decoder.substring(0, decoder.indexOf('('));
    }

    if (decoder.indexOf("BytesType") > 0) {
      return aCol.getValue(); // raw bytes
    }

    ByteBuffer valueBuff = aCol.bufferForValue();
    Object result = getColumnValue(valueBuff, fullDecoder);

    // check for indexed values
    if (m_indexedVals.containsKey(colName)) {
      HashSet<Object> vals = m_indexedVals.get(colName);

      // look for the correct index
      int foundIndex = -1;
      Object[] indexedV = vals.toArray();
      for (int i = 0; i < indexedV.length; i++) {
        if (indexedV[i].equals(result)) {
          foundIndex = i;
          break;
        }
      }

      if (foundIndex >= 0) {
        result = new Integer(foundIndex);
      } else {
        result = null; // any values that are not indexed are unknown...
      }
    }

    return result;
  }
}
