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

package com.pentaho.big.data.bundles.impl.shim.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystem;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemFactory;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by bryan on 5/28/15.
 */
public class HadoopFileSystemFactoryImpl implements HadoopFileSystemFactory {
  public static final String SHIM_IDENTIFIER = "shim.identifier";
  public static final String HDFS = "hdfs";
  private static final Logger LOGGER = LoggerFactory.getLogger( HadoopFileSystemFactoryImpl.class );
  private final boolean isActiveConfiguration;
  private final HadoopConfiguration hadoopConfiguration;
  private final String scheme;

  public HadoopFileSystemFactoryImpl( boolean isActiveConfiguration, HadoopConfiguration hadoopConfiguration,
                                      String scheme ) {
    this.isActiveConfiguration = isActiveConfiguration;
    this.hadoopConfiguration = hadoopConfiguration;
    this.scheme = scheme;
  }

  @Override public boolean canHandle( NamedCluster namedCluster ) {
    String shimIdentifier = null; // TODO: Specify shim
    return ( shimIdentifier == null && isActiveConfiguration ) || hadoopConfiguration.getIdentifier()
      .equals( shimIdentifier );
  }

  @Override public HadoopFileSystem create( NamedCluster namedCluster ) throws IOException {
    HadoopShim hadoopShim = hadoopConfiguration.getHadoopShim();
    Configuration configuration = hadoopShim.createConfiguration();
    String fsDefault;
    //TODO: AUTH
    if ( namedCluster.isMapr() ) {
      fsDefault = "maprfs:///";
    } else {
      // The connection information might be parameterized. Since we aren't tied to a transformation or job, in order to
      // use a parameter, the value would have to be set as a system property or in kettle.properties, etc.
      // Here we try to resolve the parameters if we can:
      Variables variables = new Variables();
      variables.initializeVariablesFrom( null );

      fsDefault = scheme + "://" + variables.environmentSubstitute( namedCluster.getHdfsHost() );
      String port = variables.environmentSubstitute( namedCluster.getHdfsPort() );
      if ( !Const.isEmpty( port ) ) {
        fsDefault = fsDefault + ":" + port;
      }
      if ( fsDefault.endsWith( "//" ) ) {
        fsDefault += "/";
      }
    }
    configuration.set( HadoopFileSystem.FS_DEFAULT_NAME, fsDefault );
    FileSystem fileSystem = (FileSystem) hadoopShim.getFileSystem( configuration ).getDelegate();
    if ( fileSystem instanceof LocalFileSystem ) {
      throw new IOException( "Got a local filesystem, was expecting an hdfs connection" );
    }
    return new HadoopFileSystemImpl( fileSystem );
  }
}
