/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/
package org.pentaho.big.data.kettle.plugins.hbase;

import org.mockito.Mockito;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LoggingBuffer;

import java.lang.reflect.Field;

import static org.mockito.Mockito.mock;

public class LogInjector {

  public static LoggingBuffer setMockForLoggingBuffer() throws NoSuchFieldException, IllegalAccessException {
    Field storeReflectionField = KettleLogStore.class.getDeclaredField( "store" );
    storeReflectionField.setAccessible( true );
    KettleLogStore kettleLogStoreMock = mock( KettleLogStore.class );
    storeReflectionField.set( null, kettleLogStoreMock );
    Field appenderReflectionField = KettleLogStore.class.getDeclaredField( "appender" );
    appenderReflectionField.setAccessible( true );
    LoggingBuffer loggingBuffer = Mockito.spy( new LoggingBuffer( 3 ) );
    appenderReflectionField.set( kettleLogStoreMock, loggingBuffer );
    return loggingBuffer;
  }

}
