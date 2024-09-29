/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.amazon;

import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;

/**
 * created by: rfellows date: 5/24/12
 */
public abstract class AbstractAmazonJobEntry extends JobEntryBase implements Cloneable, JobEntryInterface {

  protected String hadoopJobName;
  protected String hadoopJobFlowId;
  protected String accessKey = "";
  protected String secretKey = "";
  protected String sessionToken = "";
  protected String region;
  protected String ec2Role;
  protected String emrRole;
  protected String masterInstanceType;
  protected String slaveInstanceType;
  protected String numInstances = "2";
  protected String emrRelease;
  protected String stagingDir = "";
  protected String cmdLineArgs;
  protected boolean alive;
  protected boolean blocking;
  protected boolean runOnNewCluster = true;
  protected String loggingInterval = "60"; // 60 seconds default

  public String getHadoopJobName() {
    return hadoopJobName;
  }

  public void setHadoopJobName( String hadoopJobName ) {
    this.hadoopJobName = hadoopJobName;
  }

  public String getHadoopJobFlowId() {
    return hadoopJobFlowId;
  }

  public void setHadoopJobFlowId( String hadoopJobFlowId ) {
    this.hadoopJobFlowId = hadoopJobFlowId;
  }

  public void setAccessKey( String accessKey ) {
    this.accessKey = accessKey;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public void setSecretKey( String secretKey ) {
    this.secretKey = secretKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public void setSessionToken( String sessionToken ) {
    this.sessionToken = sessionToken;
  }

  public String getSessionToken() {
    return sessionToken;
  }

  public String getStagingDir() {
    return stagingDir;
  }

  public void setStagingDir( String stagingDir ) {
    this.stagingDir = stagingDir;
  }

  public String getRegion() {
    return region;
  }

  public void setRegion( String region ) {
    this.region = region;
  }

  public String getEc2Role() {
    return ec2Role;
  }

  public void setEc2Role( String ec2Role ) {
    this.ec2Role = ec2Role;
  }

  public String getEmrRole() {
    return emrRole;
  }

  public void setEmrRole( String emrRole ) {
    this.emrRole = emrRole;
  }

  public String getMasterInstanceType() {
    return masterInstanceType;
  }

  public void setMasterInstanceType( String masterInstanceType ) {
    this.masterInstanceType = masterInstanceType;
  }

  public String getSlaveInstanceType() {
    return slaveInstanceType;
  }

  public void setSlaveInstanceType( String slaveInstanceType ) {
    this.slaveInstanceType = slaveInstanceType;
  }

  public String getNumInstances() {
    return numInstances;
  }

  public void setNumInstances( String numInstances ) {
    this.numInstances = numInstances;
  }

  public String getEmrRelease() {
    return emrRelease;
  }

  public void setEmrRelease( String emrRelease ) {
    this.emrRelease = emrRelease;
  }

  public String getCmdLineArgs() {
    return cmdLineArgs;
  }

  public void setCmdLineArgs( String cmdLineArgs ) {
    this.cmdLineArgs = cmdLineArgs;
  }

  public boolean getAlive() {
    return alive;
  }

  public void setAlive( boolean isAlive ) {
    this.alive = isAlive;
  }

  public boolean getBlocking() {
    return blocking;
  }

  public void setBlocking( boolean blocking ) {
    this.blocking = blocking;
  }

  public boolean isRunOnNewCluster() {
    return runOnNewCluster;
  }

  public void setRunOnNewCluster( boolean runOnNewCluster ) {
    this.runOnNewCluster = runOnNewCluster;
  }

  public String getLoggingInterval() {
    return loggingInterval;
  }

  public void setLoggingInterval( String loggingInterval ) {
    this.loggingInterval = loggingInterval;
  }

  public String getAWSSecretKey() {
    return environmentSubstitute( secretKey );
  }

  public String getAWSAccessKeyId() {
    return environmentSubstitute( accessKey );
  }

  public String getAWSSessionToken() {
    return environmentSubstitute( sessionToken );
  }
}
