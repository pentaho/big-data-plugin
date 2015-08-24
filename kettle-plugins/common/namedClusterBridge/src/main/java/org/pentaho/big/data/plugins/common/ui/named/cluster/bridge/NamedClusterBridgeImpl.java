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

package org.pentaho.big.data.plugins.common.ui.named.cluster.bridge;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;

import java.util.Map;

/**
 * Created by bryan on 8/17/15.
 */
public class NamedClusterBridgeImpl implements NamedCluster {
  private final org.pentaho.di.core.namedcluster.model.NamedCluster delegate;

  public NamedClusterBridgeImpl( org.pentaho.di.core.namedcluster.model.NamedCluster delegate ) {
    this.delegate = delegate;
  }

  public static org.pentaho.di.core.namedcluster.model.NamedCluster fromOsgiNamedCluster( NamedCluster namedCluster ) {
    if ( namedCluster == null ) {
      return null;
    }
    org.pentaho.di.core.namedcluster.model.NamedCluster result =
      new org.pentaho.di.core.namedcluster.model.NamedCluster();
    new NamedClusterBridgeImpl( result ).replaceMeta( namedCluster );
    return result;
  }

  @Override public String getName() {
    return delegate.getName();
  }

  @Override public void setName( String name ) {
    delegate.setName( name );
  }

  @Override public void replaceMeta( NamedCluster nc ) {
    delegate.setName( nc.getName() );
    delegate.setHdfsHost( nc.getHdfsHost() );
    delegate.setHdfsPort( nc.getHdfsPort() );
    delegate.setHdfsUsername( nc.getHdfsUsername() );
    delegate.setHdfsPassword( nc.getHdfsPassword() );
    delegate.setJobTrackerHost( nc.getJobTrackerHost() );
    delegate.setJobTrackerPort( nc.getJobTrackerPort() );
    delegate.setZooKeeperHost( nc.getZooKeeperHost() );
    delegate.setZooKeeperPort( nc.getZooKeeperPort() );
    delegate.setOozieUrl( nc.getOozieUrl() );
    delegate.setMapr( nc.isMapr() );
    delegate.setLastModifiedDate( System.currentTimeMillis() );
  }

  @Override public String getHdfsHost() {
    return delegate.getHdfsHost();
  }

  @Override public void setHdfsHost( String hdfsHost ) {
    delegate.setHdfsHost( hdfsHost );
  }

  @Override public String getHdfsPort() {
    return delegate.getHdfsPort();
  }

  @Override public void setHdfsPort( String hdfsPort ) {
    delegate.setHdfsPort( hdfsPort );
  }

  @Override public String getHdfsUsername() {
    return delegate.getHdfsUsername();
  }

  @Override public void setHdfsUsername( String hdfsUsername ) {
    delegate.setHdfsUsername( hdfsUsername );
  }

  @Override public String getHdfsPassword() {
    return delegate.getHdfsPassword();
  }

  @Override public void setHdfsPassword( String hdfsPassword ) {
    delegate.setHdfsPassword( hdfsPassword );
  }

  @Override public String getJobTrackerHost() {
    return delegate.getJobTrackerHost();
  }

  @Override public void setJobTrackerHost( String jobTrackerHost ) {
    delegate.setJobTrackerHost( jobTrackerHost );
  }

  @Override public String getJobTrackerPort() {
    return delegate.getJobTrackerPort();
  }

  @Override public void setJobTrackerPort( String jobTrackerPort ) {
    delegate.setJobTrackerPort( jobTrackerPort );
  }

  @Override public String getZooKeeperHost() {
    return delegate.getZooKeeperHost();
  }

  @Override public void setZooKeeperHost( String zooKeeperHost ) {
    delegate.setZooKeeperHost( zooKeeperHost );
  }

  @Override public String getZooKeeperPort() {
    return delegate.getZooKeeperPort();
  }

  @Override public void setZooKeeperPort( String zooKeeperPort ) {
    delegate.setZooKeeperPort( zooKeeperPort );
  }

  @Override public String getOozieUrl() {
    return delegate.getOozieUrl();
  }

  @Override public void setOozieUrl( String oozieUrl ) {
    delegate.setOozieUrl( oozieUrl );
  }

  @Override public long getLastModifiedDate() {
    return delegate.getLastModifiedDate();
  }

  @Override public void setLastModifiedDate( long lastModifiedDate ) {
    delegate.setLastModifiedDate( lastModifiedDate );
  }

  @Override public boolean isMapr() {
    return delegate.isMapr();
  }

  @Override public void setMapr( boolean mapr ) {
    delegate.setMapr( mapr );
  }

  @Override public NamedCluster clone() {
    return new NamedClusterBridgeImpl( delegate.clone() );
  }

  @Override public void initializeVariablesFrom( VariableSpace variableSpace ) {
    delegate.initializeVariablesFrom( variableSpace );
  }

  @Override public void copyVariablesFrom( VariableSpace variableSpace ) {
    delegate.copyVariablesFrom( variableSpace );
  }

  @Override public void shareVariablesWith( VariableSpace variableSpace ) {
    delegate.shareVariablesWith( variableSpace );
  }

  @Override public VariableSpace getParentVariableSpace() {
    return delegate.getParentVariableSpace();
  }

  @Override public void setParentVariableSpace( VariableSpace variableSpace ) {
    delegate.setParentVariableSpace( variableSpace );
  }

  @Override public void setVariable( String s, String s1 ) {
    delegate.setVariable( s, s1 );
  }

  @Override public String getVariable( String s, String s1 ) {
    return delegate.getVariable( s, s1 );
  }

  @Override public String getVariable( String s ) {
    return delegate.getVariable( s );
  }

  @Override public boolean getBooleanValueOfVariable( String s, boolean b ) {
    return delegate.getBooleanValueOfVariable( s, b );
  }

  @Override public String[] listVariables() {
    return delegate.listVariables();
  }

  @Override public String environmentSubstitute( String s ) {
    return delegate.environmentSubstitute( s );
  }

  @Override public String[] environmentSubstitute( String[] strings ) {
    return delegate.environmentSubstitute( strings );
  }

  @Override public void injectVariables( Map<String, String> map ) {
    delegate.injectVariables( map );
  }

  @Override public String fieldSubstitute( String s, RowMetaInterface rowMetaInterface, Object[] objects )
    throws KettleValueException {
    return delegate.fieldSubstitute( s, rowMetaInterface, objects );
  }
}
