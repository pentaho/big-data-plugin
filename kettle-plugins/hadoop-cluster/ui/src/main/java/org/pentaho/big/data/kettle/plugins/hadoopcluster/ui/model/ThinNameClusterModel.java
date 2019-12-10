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

package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.model;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.fileupload.FileItem;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.util.List;

public class ThinNameClusterModel {
  private String name;
  private String shimVendor;
  private String shimVersion;
  private String hdfsHost;
  private String hdfsPort;
  private String hdfsUsername;
  private String hdfsPassword;
  private String jobTrackerHost;
  private String jobTrackerPort;
  private String zooKeeperHost;
  private String zooKeeperPort;
  private String oozieUrl;
  private String kafkaBootstrapServers;
  private String oldName;
  private String securityType;
  private String kerberosSubType;
  private String kerberosAuthenticationUsername;
  private String kerberosAuthenticationPassword;
  private String kerberosImpersonationUsername;
  private String kerberosImpersonationPassword;
  private String gatewayUrl;
  private String gatewayUsername;
  private String gatewayPassword;
  private String keytabAuthFile;
  private String keytabImpFile;

  private static final Logger logChannel = LoggerFactory.getLogger( ThinNameClusterModel.class );

  public String getShimVendor() {
    return shimVendor;
  }

  public void setShimVendor( String shimVendor ) {
    this.shimVendor = shimVendor;
  }

  public String getShimVersion() {
    return shimVersion;
  }

  public void setShimVersion( String shimVersion ) {
    this.shimVersion = shimVersion;
  }

  public String getHdfsHost() {
    return hdfsHost;
  }

  public void setHdfsHost( String hdfsHost ) {
    this.hdfsHost = hdfsHost;
  }

  public String getHdfsPort() {
    return hdfsPort;
  }

  public void setHdfsPort( String hdfsPort ) {
    this.hdfsPort = hdfsPort;
  }

  public String getHdfsUsername() {
    return hdfsUsername;
  }

  public void setHdfsUsername( String hdfsUsername ) {
    this.hdfsUsername = hdfsUsername;
  }

  public String getHdfsPassword() {
    return hdfsPassword;
  }

  public void setHdfsPassword( String hdfsPassword ) {
    this.hdfsPassword = hdfsPassword;
  }

  public String getJobTrackerHost() {
    return jobTrackerHost;
  }

  public void setJobTrackerHost( String jobTrackerHost ) {
    this.jobTrackerHost = jobTrackerHost;
  }

  public String getJobTrackerPort() {
    return jobTrackerPort;
  }

  public void setJobTrackerPort( String jobTrackerPort ) {
    this.jobTrackerPort = jobTrackerPort;
  }

  public String getZooKeeperHost() {
    return zooKeeperHost;
  }

  public void setZooKeeperHost( String zooKeeperHost ) {
    this.zooKeeperHost = zooKeeperHost;
  }

  public String getZooKeeperPort() {
    return zooKeeperPort;
  }

  public void setZooKeeperPort( String zooKeeperPort ) {
    this.zooKeeperPort = zooKeeperPort;
  }

  public String getKafkaBootstrapServers() {
    return kafkaBootstrapServers;
  }

  public void setKafkaBootstrapServers( String kafkaBootstrapServers ) {
    this.kafkaBootstrapServers = kafkaBootstrapServers;
  }

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getOozieUrl() {
    return oozieUrl;
  }

  public void setOozieUrl( String oozieUrl ) {
    this.oozieUrl = oozieUrl;
  }

  public String getOldName() {
    return oldName;
  }

  public void setOldName( String oldName ) {
    this.oldName = oldName;
  }

  public String getSecurityType() {
    return securityType;
  }

  public void setSecurityType( String securityType ) {
    this.securityType = securityType;
  }

  public String getKerberosSubType() {
    return kerberosSubType;
  }

  public void setKerberosSubType( String kerberosSubType ) {
    this.kerberosSubType = kerberosSubType;
  }

  public String getKerberosAuthenticationUsername() {
    return kerberosAuthenticationUsername;
  }

  public void setKerberosAuthenticationUsername( String kerberosAuthenticationUsername ) {
    this.kerberosAuthenticationUsername = kerberosAuthenticationUsername;
  }

  public String getKerberosAuthenticationPassword() {
    return kerberosAuthenticationPassword;
  }

  public void setKerberosAuthenticationPassword( String kerberosAuthenticationPassword ) {
    this.kerberosAuthenticationPassword = kerberosAuthenticationPassword;
  }

  public String getKerberosImpersonationUsername() {
    return kerberosImpersonationUsername;
  }

  public void setKerberosImpersonationUsername( String kerberosImpersonationUsername ) {
    this.kerberosImpersonationUsername = kerberosImpersonationUsername;
  }

  public String getKerberosImpersonationPassword() {
    return kerberosImpersonationPassword;
  }

  public void setKerberosImpersonationPassword( String kerberosImpersonationPassword ) {
    this.kerberosImpersonationPassword = kerberosImpersonationPassword;
  }

  public String getGatewayUrl() {
    return gatewayUrl;
  }

  public void setGatewayUrl( String gatewayUrl ) {
    this.gatewayUrl = gatewayUrl;
  }

  public String getGatewayUsername() {
    return gatewayUsername;
  }

  public void setGatewayUsername( String gatewayUsername ) {
    this.gatewayUsername = gatewayUsername;
  }

  public String getGatewayPassword() {
    return gatewayPassword;
  }

  public void setGatewayPassword( String gatewayPassword ) {
    this.gatewayPassword = gatewayPassword;
  }

  public String getKeytabAuthFile() {
    return keytabAuthFile;
  }

  public void setKeytabAuthFile( String keytabAuthFile ) {
    this.keytabAuthFile = keytabAuthFile;
  }

  public String getKeytabImpFile() {
    return keytabImpFile;
  }

  public void setKeytabImpFile( String keytabImpFile ) {
    this.keytabImpFile = keytabImpFile;
  }

  public static ThinNameClusterModel unmarshall( List<FileItem> siteFilesSource ) {
    ThinNameClusterModel model = new ThinNameClusterModel();
    try {
      FileItem siteFile = (FileItem) CollectionUtils.find( siteFilesSource, ( Object object ) -> {
        FileItem fileItem = (FileItem) object;
        return fileItem.getFieldName().equals( "data" );
      } );

      InputStreamReader inputStreamReader = new InputStreamReader( siteFile.getInputStream() );
      JSONParser parser = new JSONParser();
      JSONObject json = (JSONObject) parser.parse( inputStreamReader );
      model.setName( (String) json.get( "name" ) );
      model.setShimVendor( (String) json.get( "shimVendor" ) );
      model.setShimVersion( (String) json.get( "shimVersion" ) );
      model.setHdfsHost( (String) json.get( "hdfsHost" ) );
      model.setHdfsPort( (String) json.get( "hdfsPort" ) );
      model.setHdfsUsername( (String) json.get( "hdfsUsername" ) );
      model.setHdfsPassword( (String) json.get( "hdfsPassword" ) );
      model.setJobTrackerHost( (String) json.get( "jobTrackerHost" ) );
      model.setJobTrackerPort( (String) json.get( "jobTrackerPort" ) );
      model.setZooKeeperHost( (String) json.get( "zooKeeperHost" ) );
      model.setZooKeeperPort( (String) json.get( "zooKeeperPort" ) );
      model.setOozieUrl( (String) json.get( "oozieUrl" ) );
      model.setKafkaBootstrapServers( (String) json.get( "kafkaBootstrapServers" ) );
      model.setOldName( (String) json.get( "oldName" ) );
      model.setSecurityType( (String) json.get( "securityType" ) );
      model.setKerberosSubType( (String) json.get( "kerberosSubType" ) );
      model.setKerberosAuthenticationUsername( (String) json.get( "kerberosAuthenticationUsername" ) );
      model.setKerberosAuthenticationPassword( (String) json.get( "kerberosAuthenticationPassword" ) );
      model.setKerberosImpersonationUsername( (String) json.get( "kerberosImpersonationUsername" ) );
      model.setKerberosImpersonationPassword( (String) json.get( "kerberosImpersonationPassword" ) );
      model.setGatewayUrl( (String) json.get( "gatewayUrl" ) );
      model.setGatewayUsername( (String) json.get( "gatewayUsername" ) );
      model.setGatewayPassword( (String) json.get( "gatewayPassword" ) );
      siteFilesSource.remove( siteFile );
    } catch ( Exception e ) {
      logChannel.error( e.getMessage() );
    }
    return model;
  }
}
