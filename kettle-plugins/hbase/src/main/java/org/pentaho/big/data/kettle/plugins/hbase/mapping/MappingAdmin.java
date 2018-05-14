/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 *
 * ******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.big.data.kettle.plugins.hbase.mapping;

import org.pentaho.hadoop.shim.api.hbase.ByteConversionUtil;
import org.pentaho.hadoop.shim.api.hbase.HBaseConnection;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.hadoop.shim.api.hbase.mapping.MappingFactory;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterfaceFactory;
import org.pentaho.hadoop.shim.api.hbase.table.HBasePut;
import org.pentaho.hadoop.shim.api.hbase.table.HBaseTable;
import org.pentaho.hadoop.shim.api.hbase.table.HBaseTableWriteOperationManager;
import org.pentaho.hadoop.shim.api.hbase.Result;
import org.pentaho.hadoop.shim.api.hbase.table.ResultScanner;
import org.pentaho.hadoop.shim.api.hbase.table.ResultScannerBuilder;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

/**
 * Class for managing a mapping table in HBase. Has routines for creating the mapping table, writing and reading
 * mappings to/from the table and creating a test table for debugging purposes. Also has a rough and ready command line
 * interface. For more information on the structure of a table mapping see org.pentaho.hbase.mapping.Mapping.
 *
 * @author Mark Hall (mhall[{at]}pentaho{[dot]}com)
 */
public class MappingAdmin implements Closeable {

  /**
   * Configuration object for the connection protected Configuration m_connection;
   */

  private final HBaseConnection hBaseConnection;

  /** Name of the mapping table (might make this configurable at some stage) */
  protected String m_mappingTableName = "pentaho_mappings";

  /** family name to hold the mapped column meta data in a mapping */
  public static final String COLUMNS_FAMILY_NAME = "columns";

  /**
   * family name to hold the key meta data in a mapping. This meta data will be the same for any mapping defined on the
   * same table
   */
  public static final String KEY_FAMILY_NAME = "key";

  /**
   * Constructor. No conneciton information configured.
   */
  //  public MappingAdmin() {
  //    try {
  //      HadoopConfiguration active =
  //          HadoopConfigurationBootstrap.getHadoopConfigurationProvider().getActiveConfiguration();
  //      HBaseShim hbaseShim = active.getHBaseShim();
  //      m_bytesUtil = hbaseShim.getHBaseConnection().getBytesUtil();
  //    } catch ( Exception ex ) {
  //      // catastrophic failure if we can't obtain a concrete implementation
  //      throw new RuntimeException( ex );
  //    }
  //  }


  public MappingAdmin( HBaseConnection hBaseConnection ) {
    this.hBaseConnection = hBaseConnection;
  }

  /**
   * Set the name of the mapping table.
   *
   * @param tableName
   *          the name to use for the mapping table.
   */
  public void setMappingTableName( String tableName ) {
    m_mappingTableName = tableName;
  }

  /**
   * Get the name of the mapping table
   *
   * @return the name of the mapping table
   */
  public String getMappingTableName() {
    return m_mappingTableName;
  }

  /**
   * Creates a test mapping (in standard format) called "MarksTestMapping" for a test table called "MarksTestTable"
   *
   * @throws Exception
   *           if a problem occurs
   */
  public void createTestMapping() throws Exception {
    String keyName = "MyKey";
    String tableName = "MarksTestTable";
    String mappingName = "MarksTestMapping";

    MappingFactory mappingFactory = hBaseConnection.getMappingFactory();
    HBaseValueMetaInterfaceFactory valueMetaInterfaceFactory = hBaseConnection.getHBaseValueMetaInterfaceFactory();

    Mapping.KeyType keyType = Mapping.KeyType.LONG;
    Mapping testMapping = mappingFactory.createMapping( tableName, mappingName, keyName, keyType );

    String family1 = "Family1";
    String colA = "first_string_column";
    HBaseValueMetaInterface vm = valueMetaInterfaceFactory.createHBaseValueMetaInterface( family1, colA, colA, ValueMetaInterface.TYPE_STRING, -1, -1 );
    vm.setTableName( tableName );
    vm.setMappingName( mappingName );
    testMapping.addMappedColumn( vm, false );

    String colB = "first_unsigned_int_column";
    vm = valueMetaInterfaceFactory.createHBaseValueMetaInterface( family1, colB, colB, ValueMetaInterface.TYPE_INTEGER, -1, -1 );
    vm.setIsLongOrDouble( false );
    vm.setTableName( tableName );
    vm.setMappingName( mappingName );
    testMapping.addMappedColumn( vm, false );

    String family2 = "Family2";
    String colC = "first_indexed_column";
    vm = valueMetaInterfaceFactory.createHBaseValueMetaInterface( family2, colC, colC, ValueMetaInterface.TYPE_STRING, -1, -1 );
    vm.setTableName( tableName );
    vm.setMappingName( mappingName );
    vm.setStorageType( ValueMetaInterface.STORAGE_TYPE_INDEXED );
    Object[] vals = { "nomVal1", "nomVal2", "nomVal3" };
    vm.setIndex( vals );
    testMapping.addMappedColumn( vm, false );

    String colD = "first_binary_column";
    vm = valueMetaInterfaceFactory.createHBaseValueMetaInterface( family1, colD, colD, ValueMetaInterface.TYPE_BINARY, -1, -1 );
    vm.setTableName( tableName );
    vm.setMappingName( mappingName );
    testMapping.addMappedColumn( vm, false );

    String colE = "first_boolean_column";
    vm = valueMetaInterfaceFactory.createHBaseValueMetaInterface( family1, colE, colE, ValueMetaInterface.TYPE_BOOLEAN, -1, -1 );
    vm.setTableName( tableName );
    vm.setMappingName( mappingName );
    testMapping.addMappedColumn( vm, false );

    String colF = "first_signed_date_column";
    vm = valueMetaInterfaceFactory.createHBaseValueMetaInterface( family1, colF, colF, ValueMetaInterface.TYPE_DATE, -1, -1 );
    vm.setTableName( tableName );
    vm.setMappingName( mappingName );
    testMapping.addMappedColumn( vm, false );

    String colG = "first_signed_double_column";
    vm = valueMetaInterfaceFactory.createHBaseValueMetaInterface( family2, colG, colG, ValueMetaInterface.TYPE_NUMBER, -1, -1 );
    vm.setTableName( tableName );
    vm.setMappingName( mappingName );
    testMapping.addMappedColumn( vm, false );

    String colH = "first_signed_float_column";
    vm = valueMetaInterfaceFactory.createHBaseValueMetaInterface( family2, colH, colH, ValueMetaInterface.TYPE_NUMBER, -1, -1 );
    vm.setIsLongOrDouble( false );
    vm.setTableName( tableName );
    vm.setMappingName( mappingName );
    testMapping.addMappedColumn( vm, false );

    String colI = "first_signed_int_column";
    vm = valueMetaInterfaceFactory.createHBaseValueMetaInterface( family2, colI, colI, ValueMetaInterface.TYPE_INTEGER, -1, -1 );
    vm.setIsLongOrDouble( false );
    vm.setTableName( tableName );
    vm.setMappingName( mappingName );
    testMapping.addMappedColumn( vm, false );

    String colJ = "first_signed_long_column";
    vm = valueMetaInterfaceFactory.createHBaseValueMetaInterface( family2, colJ, colJ, ValueMetaInterface.TYPE_INTEGER, -1, -1 );
    vm.setTableName( tableName );
    vm.setMappingName( mappingName );
    testMapping.addMappedColumn( vm, false );

    String colK = "first_unsigned_date_column";
    vm = valueMetaInterfaceFactory.createHBaseValueMetaInterface( family2, colK, colK, ValueMetaInterface.TYPE_DATE, -1, -1 );
    vm.setTableName( tableName );
    vm.setMappingName( mappingName );
    testMapping.addMappedColumn( vm, false );

    String colL = "first_unsigned_double_column";
    vm = valueMetaInterfaceFactory.createHBaseValueMetaInterface( family2, colL, colL, ValueMetaInterface.TYPE_NUMBER, -1, -1 );
    vm.setTableName( tableName );
    vm.setMappingName( mappingName );
    testMapping.addMappedColumn( vm, false );

    String colM = "first_unsigned_float_column";
    vm = valueMetaInterfaceFactory.createHBaseValueMetaInterface( family2, colM, colM, ValueMetaInterface.TYPE_NUMBER, -1, -1 );
    vm.setIsLongOrDouble( false );
    vm.setTableName( tableName );
    vm.setMappingName( mappingName );
    testMapping.addMappedColumn( vm, false );

    String colN = "first_unsigned_long_column";
    vm = valueMetaInterfaceFactory.createHBaseValueMetaInterface( family2, colN, colN, ValueMetaInterface.TYPE_INTEGER, -1, -1 );
    vm.setTableName( tableName );
    vm.setMappingName( mappingName );
    testMapping.addMappedColumn( vm, false );

    putMapping( testMapping, false );
  }

  /**
   * Creates a test mapping (in tuple format) called "MarksTestTupleMapping" for a test table called
   * "MarksTestTupleTable"
   *
   * @throws Exception
   *           if a problem occurs
   */
  public void createTestTupleMapping() throws Exception {
    String keyName = "KEY";
    String tableName = "MarksTestTupleTable";
    String mappingName = "MarksTestTupleMapping";

    MappingFactory mappingFactory = hBaseConnection.getMappingFactory();
    HBaseValueMetaInterfaceFactory valueMetaInterfaceFactory = hBaseConnection.getHBaseValueMetaInterfaceFactory();

    Mapping.KeyType keyType = Mapping.KeyType.UNSIGNED_LONG;
    Mapping testMapping = mappingFactory.createMapping( tableName, mappingName, keyName, keyType );
    testMapping.setTupleMapping( true );
    String family = "";
    String colName = "";

    HBaseValueMetaInterface vm = valueMetaInterfaceFactory.createHBaseValueMetaInterface( family, colName, "Family", ValueMetaInterface.TYPE_STRING, -1, -1 );
    testMapping.addMappedColumn( vm, true );
    vm = valueMetaInterfaceFactory.createHBaseValueMetaInterface( family, colName, "Column", ValueMetaInterface.TYPE_STRING, -1, -1 );
    testMapping.addMappedColumn( vm, true );
    vm = valueMetaInterfaceFactory.createHBaseValueMetaInterface( family, colName, "Value", ValueMetaInterface.TYPE_STRING, -1, -1 );
    testMapping.addMappedColumn( vm, true );
    vm = valueMetaInterfaceFactory.createHBaseValueMetaInterface( family, colName, "Timestamp", ValueMetaInterface.TYPE_INTEGER, -1, -1 );
    vm.setIsLongOrDouble( true );
    testMapping.addMappedColumn( vm, true );

    putMapping( testMapping, false );
  }

  /**
   * Creates a test table called "MarksTestTupleTable"
   *
   * @throws Exception
   *           if a problem occurs
   */
  public void createTupleTestTable() throws Exception {
    // create a test table in the same format as the test tuple mapping
    ByteConversionUtil byteConversionUtil = hBaseConnection.getByteConversionUtil();
    if ( hBaseConnection == null ) {
      throw new IOException( "No connection exists yet!" );
    }

    HBaseTable marksTestTupleTable = hBaseConnection.getTable( "MarksTestTupleTable" );
    if ( marksTestTupleTable.exists() ) {
      // drop/delete the table and re-create
      marksTestTupleTable.disable();
      marksTestTupleTable.delete();
    }

    List<String> colFamilies = new ArrayList<String>();
    colFamilies.add( "Family1" );
    colFamilies.add( "Family2" );
    marksTestTupleTable.create( colFamilies, null );
    HBaseTableWriteOperationManager writeOperationManager =
      marksTestTupleTable.createWriteOperationManager( (long) 1024 * 1024 * 12 );

    for ( long key = 1; key < 500; key++ ) {
      HBasePut hBasePut = writeOperationManager.createPut( byteConversionUtil.encodeKeyValue( key, Mapping.KeyType.UNSIGNED_LONG ) );
      hBasePut.setWriteToWAL( false );

      // 20 columns every second row (all columns are string)
      for ( int i = 0; i < 10 * ( ( key % 2 ) + 1 ); i++ ) {
        if ( i < 10 ) {
          hBasePut.addColumn( "Family1", "string_col" + i, false, byteConversionUtil.toBytes( "StringValue_" + key ) );
        } else {
          hBasePut.addColumn( "Family2", "string_col" + i, false, byteConversionUtil.toBytes( "StringValue_" + key ) );
        }

        hBasePut.execute();
      }
    }
    writeOperationManager.flushCommits();
    writeOperationManager.close();
  }

  /**
   * Creates a test table called "MarksTestTable"
   *
   * @throws Exception
   *           if a problem occurs
   */
  public void createTestTable() throws Exception {

    // create a test table in the same format as the test mapping
    ByteConversionUtil byteConversionUtil = hBaseConnection.getByteConversionUtil();

    HBaseTable marksTestTable = hBaseConnection.getTable( "MarksTestTable" );
    if ( marksTestTable != null ) {
      // drop/delete the table and re-create
      marksTestTable.disable();
      marksTestTable.delete();
    }

    List<String> colFamilies = new ArrayList<String>();
    colFamilies.add( "Family1" );
    colFamilies.add( "Family2" );
    marksTestTable.create( colFamilies, null );
    HBaseTableWriteOperationManager writeOperationManager =
      marksTestTable.createWriteOperationManager( (long) 1024 * 1024 * 12 );

    // insert 200 test rows of random stuff
    Random r = new Random();
    String[] nomVals = { "nomVal1", "nomVal2", "nomVal3" };
    Date date = new Date();
    Calendar c = new GregorianCalendar();
    c.setTime( date );
    Calendar c2 = new GregorianCalendar();
    c2.set( 1970, 2, 1 );
    for ( long key = -500; key < 20000; key++ ) {
      HBasePut hBasePut = writeOperationManager.createPut( byteConversionUtil.encodeKeyValue( key, Mapping.KeyType.LONG ) );
      hBasePut.setWriteToWAL( false );

      // unsigned (positive) integer column

      hBasePut.addColumn( "Family1", "first_unsigned_int_column", false, byteConversionUtil.toBytes( ( key < 0
        ? (int) -key : key ) / 10 ) );

      // String column
      hBasePut
        .addColumn( "Family1", "first_string_column", false, byteConversionUtil.toBytes( "StringValue_" + key ) );

      // have some null values - every 10th row has no value for the indexed
      // column
      if ( key % 10L > 0 ) {
        int index = r.nextInt( 3 );
        String nomVal = nomVals[ index ];
        hBasePut.addColumn( "Family2", "first_indexed_column", false, byteConversionUtil.toBytes( nomVal ) );
      }

      // signed integer column
      double d = r.nextDouble();
      int signedInt = r.nextInt( 100 );
      if ( d < 0.5 ) {
        signedInt = -signedInt;
      }
      hBasePut.addColumn( "Family2", "first_signed_int_column", false, byteConversionUtil.toBytes( signedInt ) );

      // unsigned (positive) float column
      float f = r.nextFloat() * 1000.0f;
      hBasePut.addColumn( "Family2", "first_unsigned_float_column", false, byteConversionUtil.toBytes( f ) );

      // signed float column
      if ( d > 0.5 ) {
        f = -f;
      }
      hBasePut.addColumn( "Family2", "first_signed_float_column", false, byteConversionUtil.toBytes( f ) );

      // unsigned double column
      double dd = d * 10000 * r.nextDouble();
      hBasePut.addColumn( "Family2", "first_unsigned_double_column", false, byteConversionUtil.toBytes( dd ) );

      // signed double
      if ( d > 0.5 ) {
        dd = -dd;
      }
      hBasePut.addColumn( "Family2", "first_signed_double_column", false, byteConversionUtil.toBytes( dd ) );

      // unsigned long
      long l = r.nextInt( 300 );
      hBasePut.addColumn( "Family2", "first_unsigned_long_column", false, byteConversionUtil.toBytes( l ) );

      if ( d < 0.5 ) {
        l = -l;
      }
      hBasePut.addColumn( "Family2", "first_signed_long_column", false, byteConversionUtil.toBytes( l ) );

      // unsigned date (vals >= 1st Jan 1970)
      c.add( Calendar.DAY_OF_YEAR, 1 );

      long longd = c.getTimeInMillis();
      hBasePut.addColumn( "Family1", "first_unsigned_date_column", false, byteConversionUtil.toBytes( longd ) );

      // signed date (vals < 1st Jan 1970)
      c2.add( Calendar.DAY_OF_YEAR, -1 );
      longd = c2.getTimeInMillis();

      hBasePut.addColumn( "Family1", "first_signed_date_column", false, byteConversionUtil.toBytes( longd ) );

      // boolean column
      String bVal = "";
      if ( d < 0.5 ) {
        bVal = "N";
      } else {
        bVal = "Y";
      }
      hBasePut.addColumn( "Family1", "first_boolean_column", false, byteConversionUtil.toBytes( bVal ) );

      // serialized objects
      byte[] serialized = byteConversionUtil.encodeObject( new Double( d ) );

      hBasePut.addColumn( "Family1", "first_serialized_column", false, serialized );

      // binary (raw bytes)
      byte[] rawStuff = byteConversionUtil.toBytes( 5034555 );
      hBasePut.addColumn( "Family1", "first_binary_column", false, rawStuff );

      hBasePut.execute();
    }

    writeOperationManager.flushCommits();
    writeOperationManager.close();
  }

  /**
   * Create the mapping table
   *
   * @throws Exception
   *           if there is no connection specified or the mapping table already exists.
   */
  public void createMappingTable() throws Exception {
    HBaseTable hBaseTable = hBaseConnection.getTable( m_mappingTableName );
    if ( hBaseTable.exists() ) {
      throw new IOException( "Mapping table already exists!" );
    }

    List<String> colFamNames = new ArrayList<String>();
    colFamNames.add( COLUMNS_FAMILY_NAME );
    colFamNames.add( KEY_FAMILY_NAME );

    hBaseTable.create( colFamNames, null );
  }

  /**
   * Check to see if the specified mapping name exists for the specified table
   *
   * @param tableName
   *          the name of the table
   * @param mappingName
   *          the name of the mapping
   * @return true if the specified mapping exists for the specified table
   * @throws IOException
   *           if a problem occurs
   */
  public boolean mappingExists( String tableName, String mappingName ) throws Exception {
    try ( HBaseTable hBaseTable = hBaseConnection.getTable( m_mappingTableName ) ) {
      if ( hBaseTable.exists() ) {
        return hBaseTable.keyExists( hBaseConnection.getByteConversionUtil().compoundKey( tableName, mappingName ) );
      }
      return false;
    }
  }

  /**
   * Get a list of tables that have mappings. List will be empty if there are no mappings defined yet.
   *
   * @return a list of tables that have mappings.
   * @throws IOException
   *           if something goes wrong
   */
  public Set<String> getMappedTables() throws Exception {
    ByteConversionUtil byteConversionUtil = hBaseConnection.getByteConversionUtil();
    HashSet<String> tableNames = new HashSet<String>();
    try ( HBaseTable hBaseTable = hBaseConnection.getTable( m_mappingTableName ) ) {
      if ( hBaseTable.exists() ) {
        ResultScannerBuilder scannerBuilder = hBaseTable.createScannerBuilder( null, null );
        scannerBuilder.setCaching( 10 );

        try ( ResultScanner resultScanner = scannerBuilder.build() ) {
          Result next;
          while ( ( next = resultScanner.next() ) != null ) {
            byte[] rawKey = next.getRow();

            // extract the table name
            String tableName = byteConversionUtil.splitKey( rawKey )[ 0 ];
            tableNames.add( tableName.trim() );
          }
        }
      }

      return tableNames;
    }
  }

  /**
   * Get a list of mappings for the supplied table name. List will be empty if there are no mappings defined for the
   * table.
   *
   * @param tableName
   *          the table name
   * @return a list of mappings
   * @throws Exception
   *           if something goes wrong.
   */
  public List<String> getMappingNames( String tableName ) throws Exception {
    ByteConversionUtil byteConversionUtil = hBaseConnection.getByteConversionUtil();
    List<String> mappingsForTable = new ArrayList<String>();
    try ( HBaseTable hBaseTable = hBaseConnection.getTable( m_mappingTableName ) ) {
      if ( hBaseTable.exists() ) {
        ResultScannerBuilder scannerBuilder = hBaseTable.createScannerBuilder( null, null );
        scannerBuilder.setCaching( 10 );

        try ( ResultScanner resultScanner = scannerBuilder.build() ) {
          Result next;
          while ( ( next = resultScanner.next() ) != null ) {
            byte[] rowKey = next.getRow();
            String[] splitKey = byteConversionUtil.splitKey( rowKey );
            String tableN = splitKey[ 0 ];

            if ( tableName.equals( tableN ) ) {
              // extract out the mapping name
              mappingsForTable.add( splitKey[ 1 ] );
            }
          }
        }
      }
      return mappingsForTable;
    }
  }

  /**
   * Delete a mapping from the mapping table
   *
   * @param tableName
   *          name of the table in question
   * @param mappingName
   *          name of the mapping in question
   * @return true if the named mapping for the named table was deleted successfully; false if the mapping table does not
   * exist or the named mapping for the named table does not exist in the mapping table
   * @throws Exception
   *           if a problem occurs during deletion
   */
  public boolean deleteMapping( String tableName, String mappingName ) throws Exception {
    ByteConversionUtil byteConversionUtil = hBaseConnection.getByteConversionUtil();
    try ( HBaseTable hBaseTable = hBaseConnection.getTable( m_mappingTableName ) ) {
      try ( HBaseTableWriteOperationManager hBaseTableWriteOperationManager = hBaseTable
        .createWriteOperationManager( null ) ) {

        if ( !hBaseTable.exists() ) {
          // create the mapping table
          createMappingTable();
          return false; // no mapping table so nothing to delete!
        }

        if ( hBaseTable.disabled() ) {
          hBaseTable.enable();
        }

        boolean mappingExists = mappingExists( tableName, mappingName );
        if ( !mappingExists ) {
          return false; // mapping doesn't seem to exist
        }

        hBaseTableWriteOperationManager.createDelete( byteConversionUtil.compoundKey( tableName, mappingName ) )
          .execute();
        return true;
      }
    }
  }

  /**
   * Delete a mapping from the mapping table
   *
   * @param theMapping
   *          the mapping to delete
   * @return true if the mapping was deleted successfully; false if the mapping table does not exist or the suppied
   * mapping does not exist in the mapping table
   * @throws Exception
   *           if a problem occurs during deletion
   */
  public boolean deleteMapping( Mapping theMapping ) throws Exception {
    String tableName = theMapping.getTableName();
    String mappingName = theMapping.getMappingName();

    return deleteMapping( tableName, mappingName );
  }


  public void putMapping( Mapping theMapping, boolean overwrite ) throws Exception {
    String tableName = theMapping.getTableName();
    String mappingName = theMapping.getMappingName();
    Map<String, HBaseValueMetaInterface> mapping = theMapping.getMappedColumns();
    String keyName = theMapping.getKeyName();
    Mapping.KeyType keyType = theMapping.getKeyType();
    boolean isTupleMapping = theMapping.isTupleMapping();
    String tupleFamilies = theMapping.getTupleFamilies();

    ByteConversionUtil byteConversionUtil = hBaseConnection.getByteConversionUtil();
    try ( HBaseTable hBaseTable = hBaseConnection.getTable( m_mappingTableName ) ) {
      if ( !hBaseTable.exists() ) {
        // create the mapping table
        createMappingTable();
      }

      if ( hBaseTable.disabled() ) {
        hBaseTable.enable();
      }

      boolean mappingExists = mappingExists( tableName, mappingName );
      if ( mappingExists && !overwrite ) {
        throw new IOException(
          "The mapping \"" + mappingName + "\" already exists " + "for table \"" + tableName + "\"" );
      }

      if ( mappingExists ) {
        deleteMapping( tableName, mappingName );
      }

      HBaseTableWriteOperationManager writeOperationManager = hBaseTable.createWriteOperationManager( null );
      HBasePut hBasePut = writeOperationManager.createPut( byteConversionUtil.compoundKey( tableName, mappingName ) );
      hBasePut.setWriteToWAL( true );

      String family = COLUMNS_FAMILY_NAME;
      Set<String> aliases = mapping.keySet();
      for ( String alias : aliases ) {
        HBaseValueMetaInterface vm = mapping.get( alias );
        String valueType = ValueMetaInterface.typeCodes[ vm.getType() ];

        // make sure that we save the correct type name so that unsigned filtering
        // works correctly!
        if ( vm.isInteger() && vm.getIsLongOrDouble() ) {
          valueType = "Long";
        }

        if ( vm.isNumber() ) {
          if ( vm.getIsLongOrDouble() ) {
            valueType = "Double";
          } else {
            valueType = "Float";
          }
        }

        // check for nominal/indexed
        if ( vm.getStorageType() == ValueMetaInterface.STORAGE_TYPE_INDEXED && vm.isString() ) {
          Object[] labels = vm.getIndex();
          StringBuffer vals = new StringBuffer();
          vals.append( "{" );

          for ( int i = 0; i < labels.length; i++ ) {
            if ( i != labels.length - 1 ) {
              vals.append( labels[ i ].toString().trim() ).append( "," );
            } else {
              vals.append( labels[ i ].toString().trim() ).append( "}" );
            }
          }
          valueType = vals.toString();
        }

        // add this mapped column in
        hBasePut
          .addColumn( family, hBasePut.createColumnName( vm.getColumnFamily(), vm.getColumnName(), alias ), false,
            byteConversionUtil.toBytes( valueType ) );
      }

      // now do the key
      family = KEY_FAMILY_NAME;
      List<String> qualifier = new ArrayList<>( Collections.singletonList( keyName ) );

      // indicate that this is a tuple mapping by appending SEPARATOR to the name
      // of the key + any specified column families to extract from
      if ( isTupleMapping ) {
        if ( Const.isEmpty( tupleFamilies ) ) {
          qualifier.add( "" );
        } else {
          qualifier.add( tupleFamilies );
        }
      }
      String valueType = keyType.toString();

      hBasePut
        .addColumn( family, hBasePut.createColumnName( qualifier.toArray( new String[ qualifier.size() ] ) ), false,
          byteConversionUtil.toBytes( valueType ) );

      // add the row
      hBasePut.execute();
      writeOperationManager.flushCommits();
    }
  }

  /**
   * Returns a textual description of a mapping
   *
   * @param tableName
   *          the table name
   * @param mappingName
   *          the mapping name
   * @return a string describing the specified mapping on the specified table
   * @throws IOException
   *           if a problem occurs
   */
  public String describeMapping( String tableName, String mappingName ) throws Exception {

    return describeMapping( getMapping( tableName, mappingName ) );
  }

  /**
   * Returns a textual description of a mapping
   *
   * @param aMapping
   *          the mapping
   * @return a textual description of the supplied mapping object
   * @throws IOException
   *           if a problem occurs
   */
  public String describeMapping( Mapping aMapping ) throws IOException {

    return aMapping.toString();
  }

  /**
   * Get a mapping for the specified table under the specified mapping name
   *
   * @param tableName
   *          the name of the table
   * @param mappingName
   *          the name of the mapping to get for the table
   * @return a mapping for the supplied table
   * @throws Exception
   *           if a mapping by the given name does not exist for the given table
   */
  public Mapping getMapping( String tableName, String mappingName ) throws Exception {
    ByteConversionUtil byteConversionUtil = hBaseConnection.getByteConversionUtil();
    MappingFactory mappingFactory = hBaseConnection.getMappingFactory();
    HBaseValueMetaInterfaceFactory valueMetaInterfaceFactory = hBaseConnection.getHBaseValueMetaInterfaceFactory();
    try ( HBaseTable hBaseTable = hBaseConnection.getTable( m_mappingTableName ) ) {
      if ( !hBaseTable.exists() ) {

        // create the mapping table
        createMappingTable();

        throw new IOException( "Mapping \"" + tableName + "," + mappingName + "\" does not exist!" );
      }

      byte[] compoundKey = byteConversionUtil.compoundKey( tableName, mappingName );
      ResultScannerBuilder scannerBuilder = hBaseTable.createScannerBuilder( compoundKey, compoundKey );
      scannerBuilder.setCaching( 10 );

      ResultScanner resultScanner = scannerBuilder.build();
      Result result = resultScanner.next();
      if ( result == null ) {
        throw new IOException( "Mapping \"" + tableName + "," + mappingName + "\" does not exist!" );
      }

      NavigableMap<byte[], byte[]> colsInKeyFamily = result.getFamilyMap( KEY_FAMILY_NAME );

      Set<byte[]> keyCols = colsInKeyFamily.keySet();
      // should only be one key defined!!
      if ( keyCols.size() != 1 ) {
        throw new IOException( "Mapping \"" + tableName + "," + mappingName + "\" has more than one key defined!" );
      }

      byte[] keyNameB = keyCols.iterator().next();
      String decodedKeyName = byteConversionUtil.toString( keyNameB );
      byte[] keyTypeB = colsInKeyFamily.get( keyNameB );
      String decodedKeyType = byteConversionUtil.toString( keyTypeB );
      Mapping.KeyType keyType = null;

      for ( Mapping.KeyType t : Mapping.KeyType.values() ) {
        if ( decodedKeyType.equalsIgnoreCase( t.toString() ) ) {
          keyType = t;
          break;
        }
      }

      if ( keyType == null ) {
        throw new IOException( "Unrecognized type for the key column in \"" + compoundKey + "\"" );
      }

      String tupleFamilies = "";
      boolean isTupleMapping = false;
      if ( decodedKeyName.indexOf( ',' ) > 0 ) {

        isTupleMapping = true;

        if ( decodedKeyName.indexOf( ',' ) != decodedKeyName.length() - 1 ) {
          tupleFamilies = decodedKeyName.substring( decodedKeyName.indexOf( ',' ) + 1, decodedKeyName.length() );
        }
        decodedKeyName = decodedKeyName.substring( 0, decodedKeyName.indexOf( ',' ) );
      }

      Mapping resultMapping = mappingFactory.createMapping( tableName, mappingName, decodedKeyName, keyType );
      resultMapping.setTupleMapping( isTupleMapping );
      if ( !Const.isEmpty( tupleFamilies ) ) {
        resultMapping.setTupleFamilies( tupleFamilies );
      }

      Map<String, HBaseValueMetaInterface> resultCols = new TreeMap<String, HBaseValueMetaInterface>();

      // now process the mapping
      NavigableMap<byte[], byte[]> colsInMapping = result.getFamilyMap( COLUMNS_FAMILY_NAME );

      Set<byte[]> colNames = colsInMapping.keySet();

      for ( byte[] b : colNames ) {
        String decodedName = byteConversionUtil.toString( b );
        byte[] c = colsInMapping.get( b );
        if ( c == null ) {
          throw new IOException( "No type declaration for column \"" + decodedName + "\"" );
        }

        String decodedType = byteConversionUtil.toString( c );

        HBaseValueMetaInterface newMeta = null;
        if ( decodedType.equalsIgnoreCase( "Float" ) ) {
          newMeta = valueMetaInterfaceFactory
            .createHBaseValueMetaInterface( decodedName, ValueMetaInterface.TYPE_NUMBER, -1, -1 );

          // While passing through Kettle this will be represented
          // as a double
          newMeta.setIsLongOrDouble( false );
        } else if ( decodedType.equalsIgnoreCase( "Double" ) ) {
          newMeta = valueMetaInterfaceFactory
            .createHBaseValueMetaInterface( decodedName, ValueMetaInterface.TYPE_NUMBER, -1, -1 );
        } else if ( decodedType.equalsIgnoreCase( "String" ) ) {
          newMeta = valueMetaInterfaceFactory
            .createHBaseValueMetaInterface( decodedName, ValueMetaInterface.TYPE_STRING, -1, -1 );
        } else if ( decodedType.toLowerCase().startsWith( "date" ) ) {
          newMeta = valueMetaInterfaceFactory
            .createHBaseValueMetaInterface( decodedName, ValueMetaInterface.TYPE_DATE, -1, -1 );
        } else if ( decodedType.equalsIgnoreCase( "Boolean" ) ) {
          newMeta = valueMetaInterfaceFactory
            .createHBaseValueMetaInterface( decodedName, ValueMetaInterface.TYPE_BOOLEAN, -1, -1 );
        } else if ( decodedType.equalsIgnoreCase( "Integer" ) ) {
          newMeta = valueMetaInterfaceFactory
            .createHBaseValueMetaInterface( decodedName, ValueMetaInterface.TYPE_INTEGER, -1, -1 );

          // Integer in the mapping is really an integer (not a long
          // as Kettle uses internally)
          newMeta.setIsLongOrDouble( false );
        } else if ( decodedType.equalsIgnoreCase( "Long" ) ) {
          newMeta = valueMetaInterfaceFactory
            .createHBaseValueMetaInterface( decodedName, ValueMetaInterface.TYPE_INTEGER, -1, -1 );
        } else if ( decodedType.equalsIgnoreCase( "BigNumber" ) ) {
          newMeta = valueMetaInterfaceFactory
            .createHBaseValueMetaInterface( decodedName, ValueMetaInterface.TYPE_BIGNUMBER, -1, -1 );
        } else if ( decodedType.equalsIgnoreCase( "Serializable" ) ) {
          newMeta = valueMetaInterfaceFactory
            .createHBaseValueMetaInterface( decodedName, ValueMetaInterface.TYPE_SERIALIZABLE, -1, -1 );
        } else if ( decodedType.equalsIgnoreCase( "Binary" ) ) {
          newMeta = valueMetaInterfaceFactory
            .createHBaseValueMetaInterface( decodedName, ValueMetaInterface.TYPE_BINARY, -1, -1 );
        } else if ( decodedType.startsWith( "{" ) && decodedType.endsWith( "}" ) ) {
          newMeta = valueMetaInterfaceFactory
            .createHBaseValueMetaInterface( decodedName, ValueMetaInterface.TYPE_STRING, -1, -1 );

          Object[] labels = null;
          try {
            labels = byteConversionUtil.stringIndexListToObjects( decodedType );
          } catch ( IllegalArgumentException ex ) {
            throw new IOException( "Indexed/nominal type must have at least one " + "label declared" );
          }
          newMeta.setIndex( labels );
          newMeta.setStorageType( ValueMetaInterface.STORAGE_TYPE_INDEXED );
        } else {
          throw new IOException( "Unknown column type : \"" + decodedType + "\"" );
        }

        newMeta.setTableName( tableName );
        newMeta.setMappingName( mappingName );
        // check that this one doesn't have the same name as the key!
        String alias = newMeta.getAlias();
        if ( !Mapping.TupleMapping.KEY.toString().equalsIgnoreCase( alias ) ) {
          if ( resultMapping.getKeyName().equals( alias ) ) {
            throw new IOException( "Error in mapping. Column \"" + newMeta.getAlias()
              + "\" has the same name as the table key (" + resultMapping.getKeyName() + ")" );
          } else {
            resultCols.put( newMeta.getAlias(), newMeta );
          }
        }
      }

      resultMapping.setMappedColumns( resultCols );
      return resultMapping;
    }
  }

  @Override public void close() throws IOException {
    hBaseConnection.close();
  }

  public HBaseConnection getConnection() {
    return hBaseConnection;
  }

  public static String getTableNameFromVariable( BaseStepMeta stepMeta, String mappedTableName ) {
    TransMeta parentTransMeta = stepMeta.getParentStepMeta().getParentTransMeta();
    return parentTransMeta.environmentSubstitute( mappedTableName );
  }
}
