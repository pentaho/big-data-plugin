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

package org.pentaho.big.data.kettle.plugins.hbase.output;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.big.data.kettle.plugins.hbase.mapping.MappingAdmin;
import org.pentaho.big.data.kettle.plugins.hbase.output.KettleRowToHBaseTuple.FieldException;
import org.pentaho.hadoop.shim.api.hbase.ByteConversionUtil;
import org.pentaho.hadoop.shim.api.hbase.HBaseConnection;
import org.pentaho.hadoop.shim.api.hbase.HBaseService;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.hadoop.shim.api.hbase.table.HBaseDelete;
import org.pentaho.hadoop.shim.api.hbase.table.HBasePut;
import org.pentaho.hadoop.shim.api.hbase.table.HBaseTable;
import org.pentaho.hadoop.shim.api.hbase.table.HBaseTableWriteOperationManager;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * Class providing an output step for writing data to an HBase table according to meta data column/type mapping info
 * stored in a separate HBase table called "pentaho_mappings". See org.pentaho.hbase.mapping.Mapping for details on the
 * meta data format.
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class HBaseOutput extends BaseStep implements StepInterface {

  protected HBaseOutputMeta m_meta;
  protected HBaseOutputData m_data;
  private final NamedClusterServiceLocator namedClusterServiceLocator;
  private HBaseService hBaseService;
  private HBaseTableWriteOperationManager targetTableWriteOperationManager;

  public HBaseOutput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans, NamedClusterServiceLocator namedClusterServiceLocator ) {

    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
    this.namedClusterServiceLocator = namedClusterServiceLocator;
  }

  /** Configuration object for connecting to HBase */
  protected HBaseConnection m_hbAdmin;

  /** Byte utilities */
  protected ByteConversionUtil m_bytesUtil;

  /** The mapping admin object for interacting with mapping information */
  protected MappingAdmin m_mappingAdmin;

  /** The mapping information to use in order to decode HBase column values */
  protected Mapping m_tableMapping;

  /** Information from the mapping */
  protected Map<String, HBaseValueMetaInterface> m_columnsMappedByAlias;

  /** True if the target table has been connected to successfully */
  protected HBaseTable targetTable;

  /** Index of the key in the incoming fields */
  protected int m_incomingKeyIndex;

  /** The ValueMetaInterface of the incoming key field */
  protected ValueMetaInterface m_incomingKeyValueMeta;

  /** Object used when a tuple is supplied as the incoming fields */
  protected KettleRowToHBaseTuple tupleRowConverter;

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    Object[] r = getRow();

    if ( r == null ) {
      // no more input

      // clean up/close connections etc.
      // target table will be null if we haven't seen any input
      if ( targetTable != null ) {
        if ( targetTableWriteOperationManager != null ) {
          try {
            if ( !targetTableWriteOperationManager.isAutoFlush() ) {
              logBasic( BaseMessages.getString( HBaseOutputMeta.PKG, "HBaseOutput.FlushingWriteBuffer" ) );
              targetTableWriteOperationManager.flushCommits();
            }
          } catch ( Exception ex ) {
            throw new KettleException( BaseMessages.getString( HBaseOutputMeta.PKG,
                "HBaseOutput.Error.ProblemFlushingBufferedData", ex.getMessage() ), ex );
          } finally {
            try {
              targetTableWriteOperationManager.close();
            } catch ( IOException e ) {
              // Ignore
            }
          }
        }
        try {
          targetTable.close();
        } catch ( IOException e ) {
          // Ignore
        }

        try {
          logBasic( BaseMessages.getString( HBaseOutputMeta.PKG, "HBaseOutput.ClosingConnectionToTable" ) );
          targetTable = null;
          m_hbAdmin.close();
        } catch ( Exception ex ) {
          throw new KettleException( BaseMessages.getString( HBaseOutputMeta.PKG,
              "HBaseOutput.Error.ProblemWhenClosingConnection", ex.getMessage() ), ex );
        }
      }

      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;
      m_meta = (HBaseOutputMeta) smi;
      m_data = (HBaseOutputData) sdi;

      // Get the connection to HBase
      try {
        logBasic( BaseMessages.getString( HBaseOutputMeta.PKG, "HBaseOutput.ConnectingToHBase" ) );

        List<String> connectionMessages = new ArrayList<String>();
        hBaseService = namedClusterServiceLocator.getService( m_meta.getNamedCluster(), HBaseService.class );
        m_hbAdmin =
            hBaseService.getHBaseConnection( this, environmentSubstitute( m_meta.getCoreConfigURL() ),
                environmentSubstitute( m_meta.getDefaultConfigURL() ), log );
        m_bytesUtil = hBaseService.getByteConversionUtil();

        if ( connectionMessages.size() > 0 ) {
          for ( String m : connectionMessages ) {
            logBasic( m );
          }
        }
      } catch ( Exception ex ) {
        throw new KettleException( BaseMessages.getString( HBaseOutputMeta.PKG,
            "HBaseOutput.Error.UnableToObtainConnection", ex.getMessage() ), ex );
      }
      try {
        m_mappingAdmin = new MappingAdmin( m_hbAdmin );
      } catch ( Exception ex ) {
        throw new KettleException( BaseMessages.getString( HBaseOutputMeta.PKG,
            "HBaseOutput.Error.UnableToObtainConnection", ex.getMessage() ), ex );
      }

      // check on the existence and readiness of the target table
      String targetName = environmentSubstitute( m_meta.getTargetTableName() );
      if ( Utils.isEmpty( targetName ) ) {
        throw new KettleException( BaseMessages.getString( HBaseOutputMeta.PKG,
            "HBaseOutput.Error.NoTargetTableSpecified" ) );
      }
      try {
        targetTable = m_hbAdmin.getTable( targetName );
        if ( !targetTable.exists() ) {
          throw new KettleException( BaseMessages.getString( HBaseOutputMeta.PKG,
              "HBaseOutput.Error.TargetTableDoesNotExist", targetName ) );
        }

        if ( targetTable.disabled() || !targetTable.available() ) {
          throw new KettleException( BaseMessages.getString( HBaseOutputMeta.PKG,
              "HBaseOutput.Error.TargetTableIsNotAvailable", targetName ) );
        }
      } catch ( Exception ex ) {
        throw new KettleException( BaseMessages.getString( HBaseOutputMeta.PKG,
            "HBaseOutput.Error.ProblemWhenCheckingAvailReadiness", targetName, ex.getMessage() ), ex );
      }

      // Get mapping details for the target table

      if ( m_meta.getMapping() != null && Utils.isEmpty( m_meta.getTargetMappingName() ) ) {
        m_tableMapping = m_meta.getMapping();
      } else {
        try {
          logBasic( BaseMessages.getString( HBaseOutputMeta.PKG, "HBaseOutput.RetrievingMappingDetails" ) );

          m_tableMapping =
              m_mappingAdmin.getMapping( environmentSubstitute( m_meta.getTargetTableName() ), environmentSubstitute(
                  m_meta.getTargetMappingName() ) );
        } catch ( Exception ex ) {
          throw new KettleException( BaseMessages.getString( HBaseOutputMeta.PKG,
              "HBaseOutput.Error.ProblemGettingMappingInfo", ex.getMessage() ), ex );
        }
      }
      m_columnsMappedByAlias = m_tableMapping.getMappedColumns();

      if ( !m_meta.m_deleteRowKey && m_tableMapping.isTupleMapping() ) {
        /*
         * We are not executing a delete and the mapping is a tuple mapping
         * Deletes need to go through the other branch of code to decode the incoming key field index
         */
        try {
          tupleRowConverter = new KettleRowToHBaseTuple( getInputRowMeta(), m_tableMapping, m_columnsMappedByAlias );
        } catch ( Exception e ) {
          throw new KettleException( e );
        }

      } else {

        // check that all incoming fields are in the mapping.
        // fewer fields than the mapping defines is OK as long as we have
        // the key as an incoming field. Can either use strict type checking
        // or use an error stream for rows where type-conversion to the mapping
        // types fail. Probably should use an error stream - e.g. could get rows
        // with negative numeric key value where mapping specifies an unsigned key
        boolean incomingKey = false;
        RowMetaInterface inMeta = getInputRowMeta();
        for ( int i = 0; i < inMeta.size(); i++ ) {
          ValueMetaInterface vm = inMeta.getValueMeta( i );
          String inName = vm.getName();

          if ( m_tableMapping.getKeyName().equals( inName ) ) {
            incomingKey = true;
            m_incomingKeyIndex = i;
            m_incomingKeyValueMeta = vm;
            // should we check the type?
          } else {
            HBaseValueMetaInterface hvm = m_columnsMappedByAlias.get( inName.trim() );
            if ( hvm == null && !m_meta.getDeleteRowKey() ) {
              throw new KettleException( BaseMessages.getString( HBaseOutputMeta.PKG,
                  "HBaseOutput.Error.CantFindIncomingField", inName, m_tableMapping.getMappingName() ) );
            }
          }
        }

        if ( !incomingKey ) {
          throw new KettleException( BaseMessages.getString( HBaseOutputMeta.PKG,
              "HBaseOutput.Error.TableKeyNotPresentInIncomingFields", m_tableMapping.getKeyName(), m_tableMapping
                  .getMappingName() ) );
        }

      }

      try {
        logBasic( BaseMessages.getString( HBaseOutputMeta.PKG, "HBaseOutput.ConnectingToTargetTable" ) );

        // set a write buffer size (and disable auto flush)
        Long writeBufferSize = null;
        if ( !Utils.isEmpty( m_meta.getWriteBufferSize() ) ) {
          writeBufferSize = Long.parseLong( environmentSubstitute( m_meta.getWriteBufferSize() ) );

          logBasic( BaseMessages.getString( HBaseOutputMeta.PKG, "HBaseOutput.SettingWriteBuffer", writeBufferSize ) );

          if ( m_meta.getDisableWriteToWAL() ) {
            logBasic( BaseMessages.getString( HBaseOutputMeta.PKG, "HBaseOutput.DisablingWriteToWAL" ) );
          }
        }
        targetTableWriteOperationManager = targetTable.createWriteOperationManager( writeBufferSize );
      } catch ( Exception e ) {
        throw new KettleException( BaseMessages.getString( HBaseOutputMeta.PKG,
            "HBaseOutput.Error.ProblemConnectingToTargetTable", e.getMessage() ), e );
      }

      // output (downstream) is the same as input
      m_data.setOutputRowMeta( getInputRowMeta() );
    }


    if ( m_meta.getDeleteRowKey() ) {

      try {

        if ( m_incomingKeyValueMeta.isNull( r[m_incomingKeyIndex] ) ) {
          throw new KettleException( BaseMessages.getString( HBaseOutputMeta.PKG, "HBaseOutput.Error.IncomingRowHasNullKeyValue" ) );
        }

        byte[] encodedKeyBytes = m_bytesUtil.encodeKeyValue( r[m_incomingKeyIndex], m_incomingKeyValueMeta, m_tableMapping.getKeyType() );
        HBaseDelete hBaseDelete = targetTableWriteOperationManager.createDelete( encodedKeyBytes );
        hBaseDelete.execute();

      } catch ( Exception ex ) {

        if ( getStepMeta().isDoingErrorHandling() ) {
          String errorDescriptions = "";
          if ( !Utils.isEmpty( ex.getMessage() ) ) {
            errorDescriptions = ex.getMessage();
          } else {
            errorDescriptions = BaseMessages.getString( HBaseOutputMeta.PKG, "HBaseOutput.Error.ErrorCreatingDelete" );
          }
          putError( getInputRowMeta(), r, 1, errorDescriptions, m_tableMapping.getKeyName(), "HBaseOutput004" );

          return true;
        } else {
          throw new KettleException( ex );
        }
      }

    } else {
      // Put the data
      HBasePut hBasePut;

      if ( tupleRowConverter != null ) {

        try {

          hBasePut =
              tupleRowConverter.createTuplePut( targetTableWriteOperationManager, m_bytesUtil, r, !m_meta
                  .getDisableWriteToWAL() );
        } catch ( Exception ex ) {

          if ( getStepMeta().isDoingErrorHandling() ) {
            String errorDescriptions = "";
            String errorFields = "Unknown";
            if ( ex instanceof FieldException ) {
              errorFields =  ( (FieldException) ex ).getFieldString();
              errorDescriptions = BaseMessages.getString( HBaseOutputMeta.PKG, "HBaseOutput.Error.MissingFieldData", errorFields );
            } else if ( !Utils.isEmpty( ex.getMessage() ) ) {
              errorDescriptions = ex.getMessage();
            } else {
              errorDescriptions = BaseMessages.getString( HBaseOutputMeta.PKG, "HBaseOutput.Error.ErrorCreatingPut" );
            }
            putError( getInputRowMeta(), r, 1, errorDescriptions, errorFields, "HBaseOutput003" );

            return true;
          } else {
            throw new KettleException( ex );
          }

        }

      } else {

        try {
          // key must not be null
          hBasePut =
              HBaseOutputData.initializeNewPut( getInputRowMeta(), m_incomingKeyIndex, r, m_tableMapping, m_bytesUtil,
                  targetTableWriteOperationManager, !m_meta.getDisableWriteToWAL() );
          if ( hBasePut == null ) {
            String errorDescriptions =
                BaseMessages.getString( HBaseOutputMeta.PKG, "HBaseOutput.Error.IncomingRowHasNullKeyValue" );
            if ( getStepMeta().isDoingErrorHandling() ) {
              String errorFields = m_tableMapping.getKeyName();
              putError( getInputRowMeta(), r, 1, errorDescriptions, errorFields, "HBaseOutput001" );

              return true;
            } else {
              throw new KettleException( errorDescriptions );
            }
          }
        } catch ( Exception ex ) {
          throw new KettleException( BaseMessages.getString( HBaseOutputMeta.PKG,
              "HBaseOutput.Error.UnableToSetTargetTable" ), ex );
        }

        // now encode the rest of the fields. Nulls do not get inserted of course
        HBaseOutputData.addColumnsToPut( getInputRowMeta(), r, m_incomingKeyIndex, m_columnsMappedByAlias, hBasePut,
            m_bytesUtil );
      }

      try {
        hBasePut.execute();
      } catch ( Exception e ) {
        String errorDescriptions =
            BaseMessages.getString( HBaseOutputMeta.PKG, "HBaseOutput.Error.ProblemInsertingRowIntoHBase", e
                .getMessage() );
        if ( getStepMeta().isDoingErrorHandling() ) {
          String errorFields = "Unknown";
          putError( getInputRowMeta(), r, 1, errorDescriptions, errorFields, "HBaseOutput002" );
        } else {
          throw new KettleException( errorDescriptions, e );
        }
      }
    }

    // pass on the data to any downstream steps
    putRow( m_data.getOutputRowMeta(), r );

    if ( log.isRowLevel() ) {
      log.logRowlevel( toString(), "Read row #" + getLinesRead() + " : " + r );
    }

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( "Linenr " + getLinesRead() );
    }

    return true;
  }

  @Override
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    if ( super.init( smi, sdi ) ) {
      HBaseOutputMeta meta = (HBaseOutputMeta) smi;
      try {
        meta.applyInjection( this );
        return true;
      } catch ( KettleException e ) {
        logError( "Error while injecting properties", e );
      }
    }
    return false;
  }

  @Override
  public void setStopped( boolean stopped ) {
    if ( isStopped() && stopped == true ) {
      return;
    }
    super.setStopped( stopped );

    if ( stopped ) {
      if ( targetTable != null ) {
        try {
          if ( !targetTableWriteOperationManager.isAutoFlush() ) {
            logBasic( BaseMessages.getString( HBaseOutputMeta.PKG, "HBaseOutput.FlushingWriteBuffer" ) );
            targetTableWriteOperationManager.flushCommits();
          }
        } catch ( Exception ex ) {
          logError( BaseMessages.getString( HBaseOutputMeta.PKG, "HBaseOutput.Error.ProblemFlushingBufferedData", ex
              .getMessage() ), ex );
        }
      }
      if ( m_hbAdmin != null ) {
        try {
          logBasic( BaseMessages.getString( HBaseOutputMeta.PKG, "HBaseOutput.ClosingConnectionToTable" ) );
          m_hbAdmin.close();
        } catch ( Exception ex ) {
          logError( BaseMessages.getString( HBaseOutputMeta.PKG, "HBaseOutput.Error.ProblemWhenClosingConnection", ex
              .getMessage() ), ex );
        }
      }
    }
  }
}
