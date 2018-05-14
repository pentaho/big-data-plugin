/*! ******************************************************************************
 *
 * Pentaho Data Integration
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
package org.pentaho.big.data.kettle.plugins.hbase.mapping;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.big.data.kettle.plugins.hbase.HBaseConnectionException;
import org.pentaho.big.data.kettle.plugins.hbase.MappingDefinition;
import org.pentaho.big.data.kettle.plugins.hbase.MappingDefinition.MappingColumn;
import org.pentaho.big.data.kettle.plugins.hbase.input.HBaseInput;
import org.pentaho.big.data.kettle.plugins.hbase.input.Messages;
import org.pentaho.hadoop.shim.api.hbase.ByteConversionUtil;
import org.pentaho.hadoop.shim.api.hbase.HBaseConnection;
import org.pentaho.hadoop.shim.api.hbase.HBaseService;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterfaceFactory;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;

public class MappingUtils {

  public static final int TUPLE_COLUMNS_COUNT = 5;

  public static final int UNDEFINED_VALUE = -1;

  private static final Set<String> TUPLE_COLUMNS = new HashSet<String>();

  public static final String TUPLE_MAPPING_VISIBILITY = "Visibility";

  static {
    TUPLE_COLUMNS.add( Mapping.TupleMapping.KEY.toString() );
    TUPLE_COLUMNS.add( Mapping.TupleMapping.FAMILY.toString() );
    TUPLE_COLUMNS.add( Mapping.TupleMapping.COLUMN.toString() );
    TUPLE_COLUMNS.add( Mapping.TupleMapping.VALUE.toString() );
    TUPLE_COLUMNS.add( Mapping.TupleMapping.TIMESTAMP.toString() );
  }

  public static MappingAdmin getMappingAdmin( ConfigurationProducer cProducer ) throws HBaseConnectionException {
    HBaseConnection hbConnection = null;
    try {
      hbConnection = cProducer.getHBaseConnection();
      hbConnection.checkHBaseAvailable();
      return new MappingAdmin( hbConnection );
    } catch ( ClusterInitializationException | IOException e ) {
      throw new HBaseConnectionException( Messages.getString( "MappingDialog.Error.Message.UnableToConnect" ), e );
    }
  }

  public static MappingAdmin getMappingAdmin( HBaseService hBaseService, VariableSpace variableSpace, String siteConfig,
      String defaultConfig ) throws IOException {
    HBaseConnection hBaseConnection = hBaseService.getHBaseConnection( variableSpace, siteConfig, defaultConfig, null );
    return new MappingAdmin( hBaseConnection );
  }

  public static Mapping getMapping( MappingDefinition mappingDefinition, HBaseService hBaseService )
    throws KettleException {
    final String tableName = mappingDefinition.getTableName();
    // empty table name or mapping name does not force an abort
    if ( Const.isEmpty( tableName ) || Const.isEmpty( mappingDefinition.getMappingName() ) ) {
      throw new KettleException( Messages.getString( "MappingDialog.Error.Message.MissingTableMappingName" ) );
    }
    // do we have any non-empty mapping definition?
    if ( mappingDefinition.getMappingColumns() == null || mappingDefinition.getMappingColumns().isEmpty() ) {
      throw new KettleException( Messages.getString( "MappingDialog.Error.Message.NoFieldsDefined" ) );
    }

    Mapping theMapping =
        hBaseService.getMappingFactory().createMapping( tableName, mappingDefinition.getMappingName() );
    // is the mapping a tuple mapping?
    final boolean isTupleMapping = isTupleMapping( mappingDefinition );
    if ( isTupleMapping ) {
      theMapping.setTupleMapping( true );
    }

    List<MappingColumn> mappingColumns = mappingDefinition.getMappingColumns();
    // think about more specific identifier then a row number
    int columnNumber = 0;
    boolean keyDefined = false;
    for ( MappingColumn column : mappingColumns ) {
      columnNumber++;
      final String alias = column.getAlias();
      final boolean isKey = column.isKey();
      if ( isKey ) {
        if ( keyDefined ) {
          throw new KettleException( Messages.getString( "MappingDialog.Error.Message.MoreThanOneKey" ) );
        }
        keyDefined = true;
      }

      String family = null;
      if ( !Const.isEmpty( column.getColumnFamily() ) ) {
        family = column.getColumnFamily();
      } else if ( !isKey && !isTupleMapping ) {
        throw new KettleException( Messages.getString( "MappingDialog.Error.Message.FamilyIssue" ) + ": "
            + columnNumber );
      }
      String colName = null;
      if ( !Const.isEmpty( column.getColumnName() ) ) {
        colName = column.getColumnName();
      } else if ( !isKey && !isTupleMapping ) {
        throw new KettleException( Messages.getString( "MappingDialog.Error.Message.ColumnIssue" ) + ": "
            + columnNumber );
      }
      String type = null;
      if ( !Const.isEmpty( column.getType() ) ) {
        type = column.getType();
      } else {
        throw new KettleException( Messages.getString( "MappingDialog.Error.Message.TypeIssue" ) + ": "
            + columnNumber );
      }

      HBaseValueMetaInterfaceFactory valueMetaInterfaceFactory = hBaseService.getHBaseValueMetaInterfaceFactory();
      if ( isKey ) {
        if ( Const.isEmpty( alias ) ) {
          throw new KettleException( Messages.getString( "MappingDialog.Error.Message.NoAliasForKey" ) );
        }

        if ( isTupleMapping ) {
          theMapping.setKeyName( alias );
          theMapping.setTupleFamilies( family );
        } else {
          theMapping.setKeyName( alias );
        }
        HBaseValueMetaInterface valueMeta =
            valueMetaInterfaceFactory.createHBaseValueMetaInterface( null, null, alias, 0, UNDEFINED_VALUE,
                UNDEFINED_VALUE );
        valueMeta.setKey( true );
        try {
          theMapping.setKeyTypeAsString( type );
          valueMeta.setType( HBaseInput.getKettleTypeByKeyType( theMapping.getKeyType() ) );
        } catch ( Exception ex ) {
          // Ignore
        }
      } else {
        try {
          HBaseValueMetaInterface valueMeta =
              buildNonKeyValueMeta( alias, family, colName, type, column.getIndexedValues(), hBaseService );
          theMapping.addMappedColumn( valueMeta, isTupleMapping );
        } catch ( Exception ex ) {
          String message =
              Messages.getString( "MappingDialog.Error.Message1.DuplicateColumn" ) + family + "," + colName + Messages
                  .getString( "MappingDialog.Error.Message2.DuplicateColumn" );
          throw new KettleException( message );
        }
      }
    }

    if ( !keyDefined ) {
      throw new KettleException( Messages.getString( "MappingDialog.Error.Message.NoKeyDefined" ) );
    }
    return theMapping;
  }

  public static HBaseValueMetaInterface buildNonKeyValueMeta( String alias, String family, String columnName,
      String type, String indexedVals, HBaseService hBaseService ) throws KettleException {
    HBaseValueMetaInterfaceFactory valueMetaInterfaceFactory = hBaseService.getHBaseValueMetaInterfaceFactory();
    HBaseValueMetaInterface valueMeta =
        valueMetaInterfaceFactory.createHBaseValueMetaInterface( family, columnName, alias, 0, UNDEFINED_VALUE,
            UNDEFINED_VALUE );
    try {
      valueMeta.setHBaseTypeFromString( type );
      if ( valueMeta.isString() && !Const.isEmpty( indexedVals ) ) {
        ByteConversionUtil byteConversionUtil = hBaseService.getByteConversionUtil();
        Object[] vals = byteConversionUtil.stringIndexListToObjects( indexedVals );
        valueMeta.setIndex( vals );
        valueMeta.setStorageType( ValueMetaInterface.STORAGE_TYPE_INDEXED );
      }
      return valueMeta;
    } catch ( IllegalArgumentException e ) {
      throw new KettleException( e );
    }
  }

  public static boolean isTupleMapping( MappingDefinition mappingDefinition ) {
    List<MappingColumn> mappingColumns = mappingDefinition.getMappingColumns();
    int mappingSize = mappingColumns.size();
    if ( !( mappingSize == TUPLE_COLUMNS_COUNT || mappingSize == TUPLE_COLUMNS_COUNT + 1 ) ) {
      return false;
    }
    int tupleIdCount = 0;
    for ( MappingColumn column : mappingColumns ) {
      if ( isTupleMappingColumn( column.getAlias() ) ) {
        tupleIdCount++;
      }
    }
    return tupleIdCount == TUPLE_COLUMNS_COUNT || tupleIdCount == TUPLE_COLUMNS_COUNT + 1;
  }

  public static boolean isTupleMappingColumn( String columnName ) {
    return TUPLE_COLUMNS.contains( columnName ) || columnName.equals( TUPLE_MAPPING_VISIBILITY );
  }

}
