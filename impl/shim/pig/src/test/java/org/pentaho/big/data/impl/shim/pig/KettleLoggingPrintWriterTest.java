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
import org.pentaho.di.core.logging.LogChannelInterface;

import java.io.PrintStream;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 10/1/15.
 */
public class KettleLoggingPrintWriterTest {
  private LogChannelInterface logChannelInterface;
  private PrintStream printStream;
  private KettleLoggingPrintWriter kettleLoggingPrintWriter;
  private String test;
  private LogChannelInterface toStringObject;

  @Before
  public void setup() {
    logChannelInterface = mock( LogChannelInterface.class );
    printStream = mock( PrintStream.class );
    kettleLoggingPrintWriter = new KettleLoggingPrintWriter( logChannelInterface, printStream );
    test = "test";
    toStringObject = mock( LogChannelInterface.class );
    when( toStringObject.toString() ).thenReturn( test );
  }

  @Test
  public void testOneArgConstructor() {
    assertNotNull( new KettleLoggingPrintWriter( logChannelInterface ) );
  }

  @Test
  public void testPrintlnString() {
    kettleLoggingPrintWriter.println( test );
    verify( logChannelInterface ).logBasic( test );
  }

  @Test
  public void testPrintlnObject() {
    kettleLoggingPrintWriter.println( toStringObject );
    verify( logChannelInterface ).logBasic( test );
  }

  @Test
  public void testWriteString() {
    kettleLoggingPrintWriter.write( test );
    verify( logChannelInterface ).logBasic( test );
  }

  @Test
  public void testPrintString() {
    kettleLoggingPrintWriter.print( test );
    verify( logChannelInterface ).logBasic( test );
  }

  @Test
  public void testPrintObject() {
    kettleLoggingPrintWriter.print( toStringObject );
    verify( logChannelInterface ).logBasic( test );
  }

  @Test
  public void testClose() {
    kettleLoggingPrintWriter.close();
    verify( printStream ).flush();
  }
}
