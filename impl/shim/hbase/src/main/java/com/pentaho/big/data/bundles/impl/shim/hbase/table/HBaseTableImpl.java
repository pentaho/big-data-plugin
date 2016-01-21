/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package com.pentaho.big.data.bundles.impl.shim.hbase.table;

import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionHandle;
import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionPool;
import com.pentaho.big.data.bundles.impl.shim.hbase.meta.HBaseValueMetaInterfaceFactoryImpl;
import org.pentaho.bigdata.api.hbase.mapping.Mapping;
import org.pentaho.bigdata.api.hbase.table.HBaseTable;
import org.pentaho.bigdata.api.hbase.table.HBaseTableWriteOperationManager;
import org.pentaho.bigdata.api.hbase.table.ResultScannerBuilder;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hbase.shim.api.HBaseValueMeta;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * Created by bryan on 1/22/16.
 */
public class HBaseTableImpl implements HBaseTable {
  private static final Class<?> PKG = HBaseTableImpl.class;
  private final HBaseConnectionPool hBaseConnectionPool;
  private final HBaseValueMetaInterfaceFactoryImpl hBaseValueMetaInterfaceFactory;
  private final HBaseBytesUtilShim hBaseBytesUtilShim;
  private final String name;

  public HBaseTableImpl( HBaseConnectionPool hBaseConnectionPool,
                         HBaseValueMetaInterfaceFactoryImpl hBaseValueMetaInterfaceFactory,
                         HBaseBytesUtilShim hBaseBytesUtilShim, String name ) {
    this.hBaseConnectionPool = hBaseConnectionPool;
    this.hBaseValueMetaInterfaceFactory = hBaseValueMetaInterfaceFactory;
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
    this.name = name;
  }

  @Override public boolean exists() throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      return hBaseConnectionHandle.getConnection().tableExists( name );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public boolean disabled() throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      return hBaseConnectionHandle.getConnection().isTableDisabled( name );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public boolean available() throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      return hBaseConnectionHandle.getConnection().isTableAvailable( name );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public void disable() throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      hBaseConnectionHandle.getConnection().disableTable( name );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public void enable() throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      hBaseConnectionHandle.getConnection().enableTable( name );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public void delete() throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      hBaseConnectionHandle.getConnection().deleteTable( name );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public void create( List<String> colFamilyNames, Properties creationProps ) throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      hBaseConnectionHandle.getConnection().createTable( name, colFamilyNames, creationProps );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public ResultScannerBuilder createScannerBuilder( byte[] keyLowerBound, byte[] keyUpperBound ) {
    return new ResultScannerBuilderImpl( hBaseConnectionPool, hBaseValueMetaInterfaceFactory, hBaseBytesUtilShim, name,
      keyLowerBound, keyUpperBound );
  }

  @Override
  public ResultScannerBuilder createScannerBuilder( Mapping tableMapping, String dateOrNumberConversionMaskForKey,
                                                    String keyStartS, String keyStopS, String scannerCacheSize,
                                                    LogChannelInterface log, VariableSpace vars )
    throws KettleException {
    byte[] keyLowerBound = null;
    byte[] keyUpperBound = null;
    org.pentaho.hbase.shim.api.Mapping.KeyType keyType =
      org.pentaho.hbase.shim.api.Mapping.KeyType.valueOf( tableMapping.getKeyType().name() );
    // Set up the scan
    if ( !Const.isEmpty( keyStartS ) ) {
      keyStartS = vars.environmentSubstitute( keyStartS );
      String convM = dateOrNumberConversionMaskForKey;

      if ( tableMapping.getKeyType() == Mapping.KeyType.BINARY ) {
        // assume we have a hex encoded string
        keyLowerBound = HBaseValueMeta.encodeKeyValue( keyStartS, keyType, hBaseBytesUtilShim );
      } else if ( tableMapping.getKeyType() != Mapping.KeyType.STRING ) {
        // allow a conversion mask in the start key field to override any
        // specified for
        // the key in the user specified fields
        String[] parts = keyStartS.split( "@" );
        if ( parts.length == 2 ) {
          keyStartS = parts[ 0 ];
          convM = parts[ 1 ];
        }

        if ( !Const.isEmpty( convM ) && convM.length() > 0 ) {

          if ( tableMapping.getKeyType() == Mapping.KeyType.DATE
            || tableMapping.getKeyType() == Mapping.KeyType.UNSIGNED_DATE ) {
            SimpleDateFormat sdf = new SimpleDateFormat();
            sdf.applyPattern( convM );
            try {
              Date d = sdf.parse( keyStartS );
              keyLowerBound = HBaseValueMeta.encodeKeyValue( d, keyType, hBaseBytesUtilShim );
            } catch ( ParseException e ) {
              throw new KettleException( BaseMessages.getString( PKG,
                "HBaseInput.Error.UnableToParseLowerBoundKeyValue", keyStartS ), e );
            }
          } else {
            // Number type
            // Double/Float or Long/Integer
            DecimalFormat df = new DecimalFormat();
            df.applyPattern( convM );
            Number num = null;
            try {
              num = df.parse( keyStartS );
              keyLowerBound = HBaseValueMeta.encodeKeyValue( num, keyType, hBaseBytesUtilShim );
            } catch ( ParseException e ) {
              throw new KettleException( BaseMessages.getString( PKG,
                "HBaseInput.Error.UnableToParseLowerBoundKeyValue", keyStartS ), e );
            }
          }
        } else {
          // just try it as a string
          keyLowerBound = HBaseValueMeta.encodeKeyValue( keyStartS, keyType, hBaseBytesUtilShim );
        }
      } else {
        // it is a string
        keyLowerBound = HBaseValueMeta.encodeKeyValue( keyStartS, keyType, hBaseBytesUtilShim );
      }

      if ( !Const.isEmpty( keyStopS ) ) {
        keyStopS = vars.environmentSubstitute( keyStopS );
        convM = dateOrNumberConversionMaskForKey;

        if ( tableMapping.getKeyType() == Mapping.KeyType.BINARY ) {
          // assume we have a hex encoded string
          keyUpperBound = HBaseValueMeta.encodeKeyValue( keyStopS, keyType, hBaseBytesUtilShim );
        } else if ( tableMapping.getKeyType() != Mapping.KeyType.STRING ) {

          // allow a conversion mask in the stop key field to override any
          // specified for
          // the key in the user specified fields
          String[] parts = keyStopS.split( "@" );
          if ( parts.length == 2 ) {
            keyStopS = parts[ 0 ];
            convM = parts[ 1 ];
          }

          if ( !Const.isEmpty( convM ) && convM.length() > 0 ) {
            if ( tableMapping.getKeyType() == Mapping.KeyType.DATE
              || tableMapping.getKeyType() == Mapping.KeyType.UNSIGNED_DATE ) {
              SimpleDateFormat sdf = new SimpleDateFormat();
              sdf.applyPattern( convM );
              try {
                Date d = sdf.parse( keyStopS );
                keyUpperBound = HBaseValueMeta.encodeKeyValue( d, keyType, hBaseBytesUtilShim );
              } catch ( ParseException e ) {
                throw new KettleException( BaseMessages.getString( PKG,
                  "HBaseInput.Error.UnableToParseUpperBoundKeyValue", keyStopS ), e );
              }
            } else {
              // Number type
              // Double/Float or Long/Integer
              DecimalFormat df = new DecimalFormat();
              df.applyPattern( convM );
              Number num = null;
              try {
                num = df.parse( keyStopS );
                keyUpperBound = HBaseValueMeta.encodeKeyValue( num, keyType, hBaseBytesUtilShim );
              } catch ( ParseException e ) {
                throw new KettleException( BaseMessages.getString( PKG,
                  "HBaseInput.Error.UnableToParseUpperBoundKeyValue", keyStopS ), e );
              }
            }
          } else {
            // just try it as a string
            keyUpperBound = HBaseValueMeta.encodeKeyValue( keyStopS, keyType, hBaseBytesUtilShim );
          }
        } else {
          // it is a string
          keyUpperBound = HBaseValueMeta.encodeKeyValue( keyStopS, keyType, hBaseBytesUtilShim );
        }
      }
    }

    int cacheSize = 0;

    // set any user-specified scanner caching
    if ( !Const.isEmpty( scannerCacheSize ) ) {
      String temp = vars.environmentSubstitute( scannerCacheSize );
      cacheSize = Integer.parseInt( temp );

      if ( log != null ) {
        log.logBasic( BaseMessages
          .getString( PKG, "HBaseInput.Message.SettingScannerCaching", cacheSize ) );
      }
    }
    return new ResultScannerBuilderImpl( hBaseConnectionPool, hBaseValueMetaInterfaceFactory, hBaseBytesUtilShim, name,
      keyLowerBound, keyUpperBound );
  }

  @Override public List<String> getColumnFamilies() throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      return hBaseConnectionHandle.getConnection().getTableFamiles( name );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public boolean keyExists( byte[] key ) throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle( name ) ) {
      return hBaseConnectionHandle.getConnection().sourceTableRowExists( key );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public HBaseTableWriteOperationManager createWriteOperationManager( Long writeBufferSize )
    throws IOException {
    Properties targetTableProps = new Properties();
    if ( writeBufferSize != null ) {
      targetTableProps.setProperty( org.pentaho.hbase.shim.spi.HBaseConnection.HTABLE_WRITE_BUFFER_SIZE_KEY,
        writeBufferSize.toString() );
    }
    return new HBaseTableWriteOperationManagerImpl( hBaseConnectionPool.getConnectionHandle( name, targetTableProps ),
      writeBufferSize != null );
  }

  @Override public void close() throws IOException {

  }
}
