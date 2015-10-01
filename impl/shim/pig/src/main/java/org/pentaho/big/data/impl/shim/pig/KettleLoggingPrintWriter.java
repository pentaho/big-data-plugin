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

import org.pentaho.di.core.logging.LogChannelInterface;

import java.io.PrintStream;
import java.io.PrintWriter;

/**
 * An extended PrintWriter that sends output to Kettle's logging
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class KettleLoggingPrintWriter extends PrintWriter {
  private final LogChannelInterface logChannelInterface;

  public KettleLoggingPrintWriter( LogChannelInterface logChannelInterface ) {
    this( logChannelInterface, System.out );
  }

  public KettleLoggingPrintWriter( LogChannelInterface logChannelInterface, PrintStream printStream ) {
    super( printStream );
    this.logChannelInterface = logChannelInterface;
  }

  @Override
  public void println( String string ) {
    logChannelInterface.logBasic( string );
  }

  @Override
  public void println( Object obj ) {
    println( obj.toString() );
  }

  @Override
  public void write( String string ) {
    println( string );
  }

  @Override
  public void print( String string ) {
    println( string );
  }

  @Override
  public void print( Object obj ) {
    print( obj.toString() );
  }

  @Override
  public void close() {
    flush();
  }
}
