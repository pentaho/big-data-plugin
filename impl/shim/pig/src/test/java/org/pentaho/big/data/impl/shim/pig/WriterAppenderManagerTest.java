/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.pentaho.di.core.logging.Log4jFileAppender;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LogWriter;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Created by bryan on 10/1/15.
 */
public class WriterAppenderManagerTest {
  private LogChannelInterface logChannelInterface;
  private LogLevel logLevel;
  private String testName;
  private LogWriter logWriter;
  private WriterAppenderManager writerAppenderManager;

  @Before
  public void setup() {
    logChannelInterface = mock( LogChannelInterface.class );
    logLevel = LogLevel.DETAILED;
    testName = "testName";
    logWriter = mock( LogWriter.class );
    writerAppenderManager = new WriterAppenderManager( logChannelInterface, logLevel, testName, logWriter );
  }

  @Test
  public void testConstructorAndClose() throws IOException {
    ArgumentCaptor<Log4jFileAppender> captor = ArgumentCaptor.forClass( Log4jFileAppender.class );
    verify( logWriter ).addAppender( captor.capture() );
    assertNotNull( writerAppenderManager.getFile() );
    writerAppenderManager.close();
    verify( logWriter ).removeAppender( captor.getValue() );
  }

  @Test
  public void testError() throws IOException {
    ArgumentCaptor<Log4jFileAppender> captor = ArgumentCaptor.forClass( Log4jFileAppender.class );
    logWriter = mock( LogWriter.class );
    doThrow( new RuntimeException() ).when( logWriter ).addAppender( isA( Log4jFileAppender.class ) );
    writerAppenderManager = new WriterAppenderManager( logChannelInterface, logLevel, testName, logWriter );
    verify( logWriter ).addAppender( captor.capture() );
    assertNotNull( writerAppenderManager.getFile() );
    writerAppenderManager.close();
    verify( logWriter ).removeAppender( captor.getValue() );
  }

  @Test
  public void testFactory() throws IOException {
    new WriterAppenderManager.Factory().create( logChannelInterface, logLevel, testName ).close();
  }
}
