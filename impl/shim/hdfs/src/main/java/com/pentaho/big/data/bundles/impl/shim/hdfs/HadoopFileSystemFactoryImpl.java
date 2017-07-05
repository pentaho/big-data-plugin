/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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

package com.pentaho.big.data.bundles.impl.shim.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystem;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemFactory;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * Created by bryan on 5/28/15.
 */
public class HadoopFileSystemFactoryImpl implements HadoopFileSystemFactory {
  public static final String SHIM_IDENTIFIER = "shim.identifier";
  public static final String HDFS = "hdfs";
  private static final Logger LOGGER = LoggerFactory.getLogger( HadoopFileSystemFactoryImpl.class );
  private final boolean isActiveConfiguration;
  private final HadoopConfiguration hadoopConfiguration;

  public HadoopFileSystemFactoryImpl( boolean isActiveConfiguration, HadoopConfiguration hadoopConfiguration,
                                      String scheme ) {
    this.isActiveConfiguration = isActiveConfiguration;
    this.hadoopConfiguration = hadoopConfiguration;
  }

  @Override public boolean canHandle( NamedCluster namedCluster ) {
    String shimIdentifier = namedCluster.getShimIdentifier();
    //handle only if we do not use gateway
    return ( shimIdentifier == null && isActiveConfiguration && !namedCluster.isUseGateway() )
           || ( hadoopConfiguration.getIdentifier().equals( shimIdentifier ) && !namedCluster.isUseGateway() );
  }

  @Override
  public HadoopFileSystem create( NamedCluster namedCluster ) throws IOException {
    return create( namedCluster, null );
  }

  @Override
  public HadoopFileSystem create( NamedCluster namedCluster, URI uri ) throws IOException {
    final HadoopShim hadoopShim = hadoopConfiguration.getHadoopShim();
    final Configuration configuration = hadoopShim.createConfiguration();
    FileSystem fileSystem = (FileSystem) hadoopShim.getFileSystem( configuration ).getDelegate();
    if ( fileSystem instanceof LocalFileSystem ) {
      LOGGER.error(  "Got a local filesystem, was expecting an hdfs connection" );
      throw new IOException( "Got a local filesystem, was expecting an hdfs connection" );
    }

    return new HadoopFileSystemImpl( new HadoopFileSystemCallable() {
      @Override
      public FileSystem getFileSystem() {
        try {
          return (FileSystem) hadoopShim.getFileSystem( uri, configuration, null ).getDelegate();
        } catch ( IOException e ) {
          LOGGER.debug( "Error looking up/creating the file system ", e );
          return null;
        } catch ( InterruptedException e ) {
          LOGGER.debug( "Error looking up/creating the file system ", e );
          return null;
        }
      }
    } );
  }
}
