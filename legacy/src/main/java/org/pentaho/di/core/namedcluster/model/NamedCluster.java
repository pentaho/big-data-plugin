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

package org.pentaho.di.core.namedcluster.model;

import java.util.Comparator;
import java.util.Map;

import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;
import org.w3c.dom.Node;

@MetaStoreElementType( name = "NamedCluster", description = "A NamedCluster" )
public class NamedCluster implements Cloneable, VariableSpace {

  private VariableSpace variables = new Variables();

  public static final String HDFS_SCHEME = "hdfs";
  public static final String MAPRFS_SCHEME = "maprfs";
  public static final String WASB_SCHEME = "wasb";

  @MetaStoreAttribute
  private String name;

  @MetaStoreAttribute
  private String shimIdentifier;

  @MetaStoreAttribute
  private String storageScheme;

  @MetaStoreAttribute
  private String hdfsHost;
  @MetaStoreAttribute
  private String hdfsPort;
  @MetaStoreAttribute
  private String hdfsUsername;
  @MetaStoreAttribute ( password = true )
  private String hdfsPassword;

  @MetaStoreAttribute
  private String jobTrackerHost;
  @MetaStoreAttribute
  private String jobTrackerPort;

  @MetaStoreAttribute
  private String zooKeeperHost;
  @MetaStoreAttribute
  private String zooKeeperPort;

  @MetaStoreAttribute
  private String oozieUrl;

  @MetaStoreAttribute
  private boolean mapr;

  @MetaStoreAttribute
  private String gatewayUrl;

  @MetaStoreAttribute
  private String gatewayUsername;

  @MetaStoreAttribute ( password = true )
  private String gatewayPassword;

  @MetaStoreAttribute
  private boolean useGateway;

  @MetaStoreAttribute
  private String kafkaBootstrapServers;

  @MetaStoreAttribute
  private long lastModifiedDate = System.currentTimeMillis();

  // Comparator for sorting clusters alphabetically by name
  public static final Comparator<NamedCluster> comparator = new Comparator<NamedCluster>() {
    @Override
    public int compare( NamedCluster c1, NamedCluster c2 ) {
      return c1.getName().compareToIgnoreCase( c2.getName() );
    }
  };

  public NamedCluster() {
    initializeVariablesFrom( null );
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void copyVariablesFrom( VariableSpace space ) {
    variables.copyVariablesFrom( space );
  }

  public String environmentSubstitute( String aString ) {
    return variables.environmentSubstitute( aString );
  }

  public String[] environmentSubstitute( String[] aString ) {
    return variables.environmentSubstitute( aString );
  }

  public String fieldSubstitute( String aString, RowMetaInterface rowMeta, Object[] rowData )
    throws KettleValueException {
    return variables.fieldSubstitute( aString, rowMeta, rowData );
  }

  public VariableSpace getParentVariableSpace() {
    return variables.getParentVariableSpace();
  }

  public void setParentVariableSpace( VariableSpace parent ) {
    variables.setParentVariableSpace( parent );
  }

  public String getVariable( String variableName, String defaultValue ) {
    return variables.getVariable( variableName, defaultValue );
  }

  public String getVariable( String variableName ) {
    return variables.getVariable( variableName );
  }

  public boolean getBooleanValueOfVariable( String variableName, boolean defaultValue ) {
    if ( !Utils.isEmpty( variableName ) ) {
      String value = environmentSubstitute( variableName );
      if ( !Utils.isEmpty( value ) ) {
        return ValueMetaBase.convertStringToBoolean( value );
      }
    }
    return defaultValue;
  }

  public void initializeVariablesFrom( VariableSpace parent ) {
    variables.initializeVariablesFrom( parent );
  }

  public String[] listVariables() {
    return variables.listVariables();
  }

  public void setVariable( String variableName, String variableValue ) {
    variables.setVariable( variableName, variableValue );
  }

  public void shareVariablesWith( VariableSpace space ) {
    variables = space;
  }

  public void injectVariables( Map<String, String> prop ) {
    variables.injectVariables( prop );
  }

  public void replaceMeta( NamedCluster nc ) {
    this.setName( nc.getName() );
    this.setShimIdentifier( nc.getShimIdentifier() );
    this.setStorageScheme( nc.getStorageScheme() );
    this.setHdfsHost( nc.getHdfsHost() );
    this.setHdfsPort( nc.getHdfsPort() );
    this.setHdfsUsername( nc.getHdfsUsername() );
    this.setHdfsPassword( nc.getHdfsPassword() );
    this.setJobTrackerHost( nc.getJobTrackerHost() );
    this.setJobTrackerPort( nc.getJobTrackerPort() );
    this.setZooKeeperHost( nc.getZooKeeperHost() );
    this.setZooKeeperPort( nc.getZooKeeperPort() );
    this.setOozieUrl( nc.getOozieUrl() );
    this.setMapr( nc.isMapr() );
    this.lastModifiedDate = System.currentTimeMillis();
    this.setGatewayUrl( nc.getGatewayUrl() );
    this.setGatewayUsername( nc.getGatewayUsername() );
    this.setGatewayPassword( nc.getGatewayPassword() );
    this.setUseGateway( nc.isUseGateway() );
    this.setKafkaBootstrapServers( nc.getKafkaBootstrapServers() );
  }

  public NamedCluster clone() {
    NamedCluster nc = new NamedCluster();
    nc.replaceMeta( this );
    return nc;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals( Object obj ) {
    if ( this == obj ) {
      return true;
    }
    if ( obj == null ) {
      return false;
    }
    if ( getClass() != obj.getClass() ) {
      return false;
    }
    NamedCluster other = (NamedCluster) obj;
    if ( name == null ) {
      if ( other.name != null ) {
        return false;
      }
    } else if ( !name.equals( other.name ) ) {
      return false;
    }
    return true;
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

  public String getOozieUrl() {
    return oozieUrl;
  }

  public void setOozieUrl( String oozieUrl ) {
    this.oozieUrl = oozieUrl;
  }

  public long getLastModifiedDate() {
    return lastModifiedDate;
  }

  public void setLastModifiedDate( long lastModifiedDate ) {
    this.lastModifiedDate = lastModifiedDate;
  }

  public void setMapr( boolean mapr ) {
    if ( mapr ) {
      setStorageScheme( MAPRFS_SCHEME );
    }
  }

  @Deprecated
  public boolean isMapr() {
    if ( storageScheme == null ) {
      return mapr;
    } else {
      return storageScheme.equals( MAPRFS_SCHEME );
    }
  }

  public String getShimIdentifier() {
    return shimIdentifier;
  }

  public void setShimIdentifier( String shimIdentifier ) {
    this.shimIdentifier = shimIdentifier;
  }

  @Override
  public String toString() {
    return "Named cluster: " + getName();
  }

  public String getStorageScheme() {
    if ( storageScheme == null ) {
      if ( isMapr() ) {
        storageScheme = MAPRFS_SCHEME;
      } else {
        storageScheme = HDFS_SCHEME;
      }
    }
    return storageScheme;
  }

  public void setStorageScheme( String storageScheme ) {
    this.storageScheme = storageScheme;
  }


  public String toXmlForEmbed( String rootTag ) {
    // This method should only be called on the real NamedClusterImpl
    return null;
  }
  public NamedCluster fromXmlForEmbed( Node node ) {
    // This method should only be called on the real NamedClusterImpl
    return null;
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

  public boolean isUseGateway() {
    return useGateway;
  }

  public void setUseGateway( boolean useGateway ) {
    this.useGateway = useGateway;
  }

  public String getKafkaBootstrapServers() {
    return kafkaBootstrapServers;
  }

  public void setKafkaBootstrapServers( String kafkaBootstrapServers ) {
    this.kafkaBootstrapServers = kafkaBootstrapServers;
  }
}
