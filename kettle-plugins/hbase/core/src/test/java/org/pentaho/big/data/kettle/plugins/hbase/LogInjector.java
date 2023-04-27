/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
