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
  private String importPath;
  private String oldName;
  private String securityType;
  private String kerberosSubType;
  private String kerberosAuthenticationUsername;
  private String kerberosAuthenticationPassword;
  private String kerberosImpersonationUsername;
  private String kerberosImpersonationPassword;
  private String keytabAuthenticationLocation;
  private String keytabImpersonationLocation;

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

  public String getImportPath() {
    return importPath;
  }

  public void setImportPath( String importPath ) {
    this.importPath = importPath;
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

  public String getKeytabAuthenticationLocation() {
    return keytabAuthenticationLocation;
  }

  public void setKeytabAuthenticationLocation( String keytabAuthenticationLocation ) {
    this.keytabAuthenticationLocation = keytabAuthenticationLocation;
  }

  public String getKeytabImpersonationLocation() {
    return keytabImpersonationLocation;
  }

  public void setKeytabImpersonationLocation( String keytabImpersonationLocation ) {
    this.keytabImpersonationLocation = keytabImpersonationLocation;
  }
}
