/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.model;

import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints.CachedFileItemStream;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;

import java.io.InputStreamReader;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ThinNameClusterModel {
  private static final LogChannelInterface log =
    KettleLogStore.getLogChannelInterfaceFactory().create( "ThinNameClusterModel" );

  public static final String NAME_KEY = "name";

  private String name;
  private String shimIdentifier;
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
  private List<SimpleImmutableEntry<String, String>> siteFiles;

  public void setShimIdentifier(String shimIdentifier ) {
    this.shimIdentifier = shimIdentifier;
  }

  public String getShimIdentifier() {
    return shimIdentifier;
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

  public List<SimpleImmutableEntry<String, String>> getSiteFiles() {
    return siteFiles;
  }

  public void setSiteFiles( List<SimpleImmutableEntry<String, String>> siteFiles ) {
    this.siteFiles = siteFiles;
  }

  public static ThinNameClusterModel unmarshall( Map<String, CachedFileItemStream> siteFilesSource ) {
    ThinNameClusterModel model = new ThinNameClusterModel();
    try {
      final CachedFileItemStream fileItemStream = siteFilesSource.remove( "data" );

      InputStreamReader inputStreamReader = new InputStreamReader( fileItemStream.getCachedInputStream() );
      JSONParser parser = new JSONParser();
      JSONObject json = (JSONObject) parser.parse( inputStreamReader );
      model.setName( (String) json.get( "name" ) );
      model.setShimIdentifier( (String) json.get( "shimIdentifier" ) );
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
      model.setKeytabImpFile( (String) json.get( "keytabImpFile" ) );
      model.setKeytabAuthFile( (String) json.get( "keytabAuthFile" ) );
      model.setSiteFiles( siteFilesSource.keySet().stream()
        .map( name -> new SimpleImmutableEntry<>( NAME_KEY, name ) )
        .collect( Collectors.toList() ) );
    } catch ( Exception e ) {
      log.logError( e.getMessage() );
    }
    return model;
  }
}
