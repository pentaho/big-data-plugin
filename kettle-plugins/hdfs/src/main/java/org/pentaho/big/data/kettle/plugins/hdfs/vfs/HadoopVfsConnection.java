/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hdfs.vfs;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;

/**
 * @author Tatsiana_Kasiankova
 *
 */
public class HadoopVfsConnection {

  private static final String COLON = ":";

  private static final String EMPTY = "";

  private static final String SCHEME_NAME = "hdfs";

  private String hostname;
  private String port;
  private String username;
  private String password;

  public HadoopVfsConnection( String ncHostname, String ncPort, String ncUsername, String ncPassword ) {
    super();
    this.hostname = ncHostname;
    this.port = ncPort;
    this.username = ncUsername;
    this.password = ncPassword;
  }

  public HadoopVfsConnection() {
    this( EMPTY, EMPTY, EMPTY, EMPTY );
  }

  public HadoopVfsConnection( NamedCluster nCluster, VariableSpace vs ) {
    this( EMPTY, EMPTY, EMPTY, EMPTY );
    loadNamedCluster( nCluster, vs );
  }

  /**
   * Build an HDFS URL given a URL and Port provided by the user.
   *
   * @return a String containing the HDFS URL
   */
  public String getConnectionString( String schemeName ) {
    if ( Schemes.MAPRFS_SCHEME.equals( schemeName ) ) {
      return Schemes.MAPRFS_SCHEME.concat( "://" );
    }
    StringBuffer urlString =
        new StringBuffer( !Utils.isEmpty( schemeName ) ? schemeName : SCHEME_NAME ).append( "://" );
    if ( !Utils.isEmpty( getUsername() ) ) {
      urlString.append( getUsername() ).append( COLON ).append( getPassword() ).append( "@" );
    }

    urlString.append( getHostname() );
    if ( !Utils.isEmpty( getPort() ) ) {
      urlString.append( COLON ).append( getPort() );
    }
    return urlString.toString();
  }

  private void loadNamedCluster( NamedCluster nCluster, VariableSpace vs ) {
    if ( nCluster != null ) {
      hostname = nCluster.getHdfsHost() != null ? nCluster.getHdfsHost() : EMPTY;
      port = nCluster.getHdfsPort() != null ? nCluster.getHdfsPort() : EMPTY;
      username = nCluster.getHdfsUsername() != null ? nCluster.getHdfsUsername() : EMPTY;
      password = nCluster.getHdfsPassword() != null ? nCluster.getHdfsPassword() : EMPTY;

      hostname = vs.environmentSubstitute( hostname );
      port = vs.environmentSubstitute( port );
      username = vs.environmentSubstitute( username );
      password = vs.environmentSubstitute( password );
    }
  }

  public void setCustomParameters( Props pr ) {
    pr.setCustomParameter( "HadoopVfsFileChooserDialog.host", getHostname() );
    pr.setCustomParameter( "HadoopVfsFileChooserDialog.port", getPort() );
    pr.setCustomParameter( "HadoopVfsFileChooserDialog.user", getUsername() );
    pr.setCustomParameter( "HadoopVfsFileChooserDialog.password", getPassword() );
  }

  public String getHostname() {
    return hostname;
  }

  public String getPort() {
    return port;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

}
