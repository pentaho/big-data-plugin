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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.vfs2.FileObject;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.pig.PigResult;
import org.pentaho.bigdata.api.pig.PigService;
import org.pentaho.bigdata.api.pig.impl.PigResultImpl;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.PigShim;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by bryan on 7/6/15.
 */
public class PigServiceImpl implements PigService {
  private static final Class<?> PKG = PigServiceImpl.class;
  private final NamedCluster namedCluster;
  private final PigShim pigShim;
  private final HadoopShim hadoopShim;
  private final WriterAppenderManager.Factory writerAppenderManagerFactory;
  @VisibleForTesting
  public static final String[] PIG_LOGGERS = new String[] { "org.apache.pig" };

  public PigServiceImpl( NamedCluster namedCluster, PigShim pigShim, HadoopShim hadoopShim ) {
    this( namedCluster, pigShim, hadoopShim, new WriterAppenderManager.Factory() );
  }

  public PigServiceImpl( NamedCluster namedCluster, PigShim pigShim, HadoopShim hadoopShim,
                         WriterAppenderManager.Factory writerAppenderManagerFactory ) {
    this.namedCluster = namedCluster;
    this.pigShim = pigShim;
    this.hadoopShim = hadoopShim;
    this.writerAppenderManagerFactory = writerAppenderManagerFactory;
  }

  @Override public boolean isLocalExecutionSupported() {
    return pigShim.isLocalExecutionSupported();
  }

  @Override
  public PigResult executeScript( String scriptPath, ExecutionMode executionMode, List<String> parameters, String name,
                                  LogChannelInterface logChannelInterface, VariableSpace variableSpace,
                                  LogLevel logLevel ) {
    FileObject appenderFile = null;
    try ( WriterAppenderManager appenderManager = writerAppenderManagerFactory.create( logChannelInterface, logLevel,
      name, PIG_LOGGERS ) ) {
      appenderFile = appenderManager.getFile();
      Configuration configuration = hadoopShim.createConfiguration();
      if ( executionMode != ExecutionMode.LOCAL ) {
        List<String> configMessages = new ArrayList<String>();
        hadoopShim.configureConnectionInformation( variableSpace.environmentSubstitute( namedCluster.getHdfsHost() ),
          variableSpace.environmentSubstitute( namedCluster.getHdfsPort() ),
          variableSpace.environmentSubstitute( namedCluster.getJobTrackerHost() ),
          variableSpace.environmentSubstitute( namedCluster.getJobTrackerPort() ), configuration,
          configMessages );
        if ( logChannelInterface != null ) {
          for ( String configMessage : configMessages ) {
            logChannelInterface.logBasic( configMessage );
          }
        }
      }
      URL scriptU;
      String scriptFileS = scriptPath;
      scriptFileS = variableSpace.environmentSubstitute( scriptFileS );
      if ( scriptFileS.indexOf( "://" ) == -1 ) {
        File scriptFile = new File( scriptFileS );
        scriptU = scriptFile.toURI().toURL();
      } else {
        scriptU = new URL( scriptFileS );
      }
      String pigScript = pigShim.substituteParameters( scriptU, parameters );
      Properties properties = new Properties();
      pigShim.configure( properties, executionMode == ExecutionMode.LOCAL ? null : configuration );
      return new PigResultImpl( appenderFile,
        pigShim.executeScript( pigScript, executionMode == ExecutionMode.LOCAL ? PigShim.ExecutionMode.LOCAL
          : PigShim.ExecutionMode.MAPREDUCE, properties ), null );
    } catch ( Exception e ) {
      return new PigResultImpl( appenderFile, null, e );
    }
  }
}
