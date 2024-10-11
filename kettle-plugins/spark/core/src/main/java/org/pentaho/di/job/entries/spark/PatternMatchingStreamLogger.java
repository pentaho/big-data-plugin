/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.job.entries.spark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicBoolean;

import org.pentaho.di.core.logging.LogChannelInterface;

/**
 * Class pumps input stream to output stream while searching it's content for patterns and notifying listener if any.
 *
 * @author Pavel Sakun
 */
public class PatternMatchingStreamLogger implements Runnable {
  private LogChannelInterface log;
  private InputStream is;
  private String[] patterns;
  private PatternMatchedListener listener;
  private AtomicBoolean stop;

  public PatternMatchingStreamLogger( LogChannelInterface log, InputStream is, String[] patterns, AtomicBoolean stop ) {
    this.log = log;
    this.is = is;
    this.patterns = patterns;
    this.stop = stop;
  }

  public void run() {
    BufferedReader br = new BufferedReader( new InputStreamReader( is ) );
    String line;

    try {
      while ( !stop.get() && ( line = br.readLine() ) != null ) {
        log.logBasic( line );
        for ( String pattern : patterns ) {
          if ( line.contains( pattern ) ) {
            if ( listener != null ) {
              listener.onPatternFound( pattern );
            }
          }
        }
      }
    } catch ( IOException e ) {
      log.logError( "", e );
    }
  }

  public void addPatternMatchedListener( PatternMatchedListener pml ) {
    listener = pml;
  }

  public static interface PatternMatchedListener {
    public void onPatternFound( String pattern );
  }
}
