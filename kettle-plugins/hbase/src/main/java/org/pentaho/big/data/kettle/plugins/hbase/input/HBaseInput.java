/*******************************************************************************
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

package org.pentaho.big.data.kettle.plugins.hbase.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.big.data.kettle.plugins.hbase.mapping.HBaseRowToKettleTuple;
import org.pentaho.big.data.kettle.plugins.hbase.mapping.MappingAdmin;
import org.pentaho.hadoop.shim.api.hbase.ByteConversionUtil;
import org.pentaho.hadoop.shim.api.hbase.HBaseConnection;
import org.pentaho.hadoop.shim.api.hbase.HBaseService;
import org.pentaho.hadoop.shim.api.hbase.Result;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterfaceFactory;
import org.pentaho.hadoop.shim.api.hbase.table.HBaseTable;
import org.pentaho.hadoop.shim.api.hbase.table.ResultScanner;
import org.pentaho.hadoop.shim.api.hbase.table.ResultScannerBuilder;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * Class providing an input step for reading data from an HBase table according to meta data mapping info stored in a
 * separate HBase table called "pentaho_mappings". See org.pentaho.hbase.mapping.Mapping for details on the meta data
 * format.
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class HBaseInput extends BaseStep implements StepInterface {
  private final NamedClusterServiceLocator namedClusterServiceLocator;

  protected HBaseInputMeta m_meta;
  protected HBaseInputData m_data;
  private HBaseService hBaseService;
  private HBaseTable m_hbAdminTable;
  private ResultScanner resultScanner;
  private HBaseValueMetaInterfaceFactory hBaseValueMetaInterfaceFactory;

  public HBaseInput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                     Trans trans, NamedClusterServiceLocator namedClusterServiceLocator ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
    this.namedClusterServiceLocator = namedClusterServiceLocator;
  }

  /** Connection/admin object for interacting with HBase */
  protected HBaseConnection m_hbAdmin;

  /** Byte utilities */
  protected ByteConversionUtil m_bytesUtil;

  /** The mapping admin object for interacting with mapping information */
  protected MappingAdmin m_mappingAdmin;

  /** The mapping information to use in order to decode HBase column values */
  protected Mapping m_tableMapping;

  /** Information from the mapping */
  protected Map<String, HBaseValueMetaInterface> m_columnsMappedByAlias;

  /** User-selected columns from the mapping (null indicates output all columns) */
  protected List<HBaseValueMetaInterface> m_userOutputColumns;

  /**
   * Used when decoding columns to <key, family, column, value, time stamp> tuples
   */
  protected HBaseRowToKettleTuple m_tupleHandler;

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    if ( first ) {
      first = false;
      m_meta = (HBaseInputMeta) smi;
      m_data = (HBaseInputData) sdi;

      // Get the connection to HBase
      try {
        List<String> connectionMessages = new ArrayList<String>();
        hBaseService = namedClusterServiceLocator.getService( m_meta.getNamedCluster(), HBaseService.class );
        m_hbAdmin = hBaseService.getHBaseConnection( this, environmentSubstitute( m_meta.getCoreConfigURL() ),
          environmentSubstitute( m_meta.getDefaultConfigURL() ), log );
        m_bytesUtil = hBaseService.getByteConversionUtil();
        hBaseValueMetaInterfaceFactory = hBaseService.getHBaseValueMetaInterfaceFactory();

        if ( connectionMessages.size() > 0 ) {
          for ( String m : connectionMessages ) {
            logBasic( m );
          }
        }
      } catch ( Exception ex ) {
        throw new KettleException( BaseMessages.getString( HBaseInputMeta.PKG,
            "HBaseInput.Error.UnableToObtainConnection" ), ex );
      }
      try {
        m_mappingAdmin = new MappingAdmin( m_hbAdmin );
      } catch ( Exception ex ) {
        throw new KettleException( BaseMessages.getString( HBaseInputMeta.PKG,
            "HBaseInput.Error.UnableToCreateAMappingAdminConnection" ), ex );
      }

      // check on the existence and readiness of the target table
      String sourceName = environmentSubstitute( m_meta.getSourceTableName() );
      if ( StringUtil.isEmpty( sourceName ) ) {
        throw new KettleException( BaseMessages.getString( HBaseInputMeta.PKG, "HBaseInput.TableName.Missing" ) );
      }
      HBaseTable hBaseTable;
      try {
        hBaseTable = m_hbAdmin.getTable( sourceName );
      } catch ( IOException e ) {
        throw new KettleException( BaseMessages.getString( HBaseInputMeta.PKG, "HBaseInput.Error.CantGetTable", sourceName ), e );
      }
      try {
        if ( !hBaseTable.exists() ) {
          throw new KettleException( BaseMessages.getString( HBaseInputMeta.PKG,
              "HBaseInput.Error.SourceTableDoesNotExist", sourceName ) );
        }

        if ( hBaseTable.disabled() || !hBaseTable.available() ) {
          throw new KettleException( BaseMessages.getString( HBaseInputMeta.PKG,
              "HBaseInput.Error.SourceTableIsNotAvailable", sourceName ) );
        }
      } catch ( Exception ex ) {
        throw new KettleException( BaseMessages.getString( HBaseInputMeta.PKG,
            "HBaseInput.Error.AvailabilityReadinessProblem", sourceName ), ex );
      }

      if ( m_meta.getMapping() != null && Const.isEmpty( m_meta.getSourceMappingName() ) ) {
        // use embedded mapping
        m_tableMapping = m_meta.getMapping();
      } else {
        // Otherwise get mapping details for the source table from HBase
        if ( Const.isEmpty( m_meta.getSourceMappingName() ) ) {
          throw new KettleException( BaseMessages.getString( HBaseInputMeta.PKG, "HBaseInput.Error.NoMappingName" ) );
        }
        try {
          m_tableMapping =
              m_mappingAdmin.getMapping( environmentSubstitute( m_meta.getSourceTableName() ),
                  environmentSubstitute( m_meta.getSourceMappingName() ) );
        } catch ( Exception ex ) {
          throw new KettleException( BaseMessages.getString( HBaseInputMeta.PKG,
              "HBaseInput.Error.UnableToRetrieveMapping", environmentSubstitute( m_meta.getSourceMappingName() ),
              environmentSubstitute( m_meta.getSourceTableName() ) ), ex );
        }
      }
      HBaseValueMetaInterface vm2 = hBaseValueMetaInterfaceFactory
        .createHBaseValueMetaInterface( null, null, m_tableMapping.getKeyName(),
          getKettleTypeByKeyType( m_tableMapping.getKeyType() ), -1, -1 );
      vm2.setKey( true );
      try {
        m_tableMapping.addMappedColumn( vm2, m_tableMapping.isTupleMapping() );
      } catch ( Exception exception ) {
        exception.printStackTrace();
      }
      m_columnsMappedByAlias = m_tableMapping.getMappedColumns();

      if ( m_tableMapping.isTupleMapping() ) {
        m_tupleHandler = new HBaseRowToKettleTuple( m_bytesUtil );
      }

      // conversion mask to use for user specified key values in range scan.
      // This can come from user-specified field information OR it can be
      // provided in the keyStart/keyStop values by suffixing the value with
      // "@converionMask"
      String dateOrNumberConversionMaskForKey = null;

      // if there are any user-chosen output fields in the meta data then
      // check them against table mapping. All selected fields must be present
      // in the mapping
      m_userOutputColumns = m_meta.getOutputFields();
      if ( m_userOutputColumns != null && m_userOutputColumns.size() > 0 ) {
        for ( HBaseValueMetaInterface vm : m_userOutputColumns ) {
          if ( !vm.isKey() ) {
            if ( m_columnsMappedByAlias.get( vm.getAlias() ) == null ) {
              throw new KettleException( BaseMessages.getString( HBaseInputMeta.PKG,
                "HBaseInput.Error.UnableToFindUserSelectedColumn", vm.getAlias(), m_tableMapping.getFriendlyName() ) );
            }
          } else {
            dateOrNumberConversionMaskForKey = vm.getConversionMask();
          }
        }
      }

      try {
        m_hbAdminTable = m_hbAdmin.getTable( sourceName );
      } catch ( Exception ex ) {
        throw new KettleException( BaseMessages.getString( HBaseInputMeta.PKG,
            "HBaseInput.Error.UnableToSetSourceTableForScan" ), ex );
      }

      ResultScannerBuilder scannerBuilder = m_hbAdminTable
        .createScannerBuilder( m_tableMapping, dateOrNumberConversionMaskForKey, m_meta.getKeyStartValue(),
          m_meta.getKeyStopValue(), m_meta.getScannerCacheSize(), log, this );

      // LIMIT THE SCAN TO JUST THE COLUMNS IN THE MAPPING
      // User-selected output columns?
      if ( m_userOutputColumns != null && m_userOutputColumns.size() > 0 && !m_tableMapping.isTupleMapping() ) {
        HBaseInputData.setScanColumns( scannerBuilder, m_userOutputColumns, m_tableMapping );
      }

      // set any filters
      if ( m_meta.getColumnFilters() != null && m_meta.getColumnFilters().size() > 0 ) {
        HBaseInputData.setScanFilters( scannerBuilder, m_meta.getColumnFilters(), m_meta.getMatchAnyFilter(),
          m_columnsMappedByAlias, this );
      }

      if ( !isStopped() ) {
        try {
          resultScanner = scannerBuilder.build();
        } catch ( Exception e ) {
          throw new KettleException( BaseMessages.getString( HBaseInputMeta.PKG,
              "HBaseInput.Error.UnableToExecuteSourceTableScan" ), e );
        }

        // set up the output fields (using the mapping)
        m_data.setOutputRowMeta( new RowMeta() );
        m_meta.getFields( m_data.getOutputRowMeta(), getStepname(), null, null, this, repository, metaStore );
      }
    }

    Result next = null;
    if ( !isStopped() ) {
      try {
        next = resultScanner.next();
      } catch ( Exception e ) {
        throw new KettleException( e.getMessage(), e );
      }
    }

    if ( next == null ) {
      try {
        m_hbAdminTable.close();
        m_hbAdmin.close();
      } catch ( Exception e ) {
        throw new KettleException( BaseMessages.getString( HBaseInputMeta.PKG,
            "HBaseInput.Error.ProblemClosingConnection", e.getMessage() ), e );
      }
      setOutputDone();
      return false;
    }

    if ( m_tableMapping.isTupleMapping() ) {
      List<Object[]> tupleRows =
          HBaseInputData.getTupleOutputRows( hBaseService, next, m_userOutputColumns, m_columnsMappedByAlias, m_tableMapping,
              m_tupleHandler, m_data.getOutputRowMeta() );

      for ( Object[] tuple : tupleRows ) {
        putRow( m_data.getOutputRowMeta(), tuple );
      }
      return true;
    } else {
      Object[] outRowData =
          HBaseInputData.getOutputRow( next, m_userOutputColumns, m_columnsMappedByAlias, m_tableMapping, m_data
              .getOutputRowMeta() );
      putRow( m_data.getOutputRowMeta(), outRowData );
      return true;
    }
  }

  @Override
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    if ( super.init( smi, sdi ) ) {
      HBaseInputMeta meta = (HBaseInputMeta) smi;
      try {
        meta.applyInjection( this );
        return true;
      } catch ( KettleException e ) {
        logError( "Error while injecting properties", e );
      }
    }
    return false;
  }

  public static int getKettleTypeByKeyType( Mapping.KeyType keyType ) {
    if ( keyType == null ) {
      return ValueMetaInterface.TYPE_NONE;
    }
    switch ( keyType ) {
      case BINARY:
        return ValueMetaInterface.TYPE_BINARY;
      case STRING:
        return ValueMetaInterface.TYPE_STRING;
      case UNSIGNED_LONG:
      case UNSIGNED_INTEGER:
      case LONG:
      case INTEGER:
        return ValueMetaInterface.TYPE_NUMBER;
      case UNSIGNED_DATE:
      case DATE:
        return ValueMetaInterface.TYPE_DATE;
      default:
        return ValueMetaInterface.TYPE_NONE;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.di.trans.step.BaseStep#setStopped(boolean)
   */
  @Override
  public void setStopped( boolean stopped ) {
    if ( isStopped() && stopped == true ) {
      return;
    }
    super.setStopped( stopped );

    if ( stopped && m_hbAdmin != null ) {
      logBasic( BaseMessages.getString( HBaseInputMeta.PKG, "HBaseInput.ClosingConnection" ) );
      try {
        m_hbAdmin.close();
      } catch ( IOException ex ) {
        logError( BaseMessages.getString( HBaseInputMeta.PKG, "HBaseInput.Error.ProblemClosingConnection1", ex ) );
      }
    }
  }
}
