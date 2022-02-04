/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2022 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.impl.shim.pig;

import org.apache.logging.log4j.core.Appender;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * Created by bryan on 10/1/15.
 */
public class WriterAppenderManagerTest {
  private LogChannelInterface logChannelInterface;
  private LogLevel logLevel;
  private String testName;
  private WriterAppenderManager writerAppenderManager;

  @Before
  public void setup() {
    logChannelInterface = mock( LogChannelInterface.class );
    logLevel = LogLevel.DETAILED;
    testName = "testName";
    writerAppenderManager = new WriterAppenderManager( logChannelInterface, logLevel, testName, new String[0] );
  }

  @Test
  public void testConstructorAndClose() throws IOException {
    assertNotNull( writerAppenderManager.getFile() );
    writerAppenderManager.close();
  }

  @Test
  public void testError() throws IOException {
    ArgumentCaptor<Appender> captor = ArgumentCaptor.forClass( Appender.class );
    writerAppenderManager = new WriterAppenderManager( logChannelInterface, logLevel, testName, new String[0] );
    assertNotNull( writerAppenderManager.getFile() );
    writerAppenderManager.close();
  }

  @Test
  public void testFactory() throws IOException {
    WriterAppenderManager writerAppenderManager =
      new WriterAppenderManager.Factory().create( logChannelInterface, logLevel, testName, new String[0] );
    assertNotNull( writerAppenderManager.getFile() );
    writerAppenderManager.close();
  }
}
