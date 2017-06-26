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

package org.pentaho.big.data.impl.cluster;

import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.beanutils.BeanMap;
import org.apache.commons.beanutils.BeanUtils;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.security.Base64TwoWayPasswordEncoder;
import org.pentaho.metastore.api.security.ITwoWayPasswordEncoder;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

@MetaStoreElementType( name = "NamedCluster", description = "A NamedCluster" )
public class NamedClusterImpl implements NamedCluster {

  public static final String HDFS_SCHEME = "hdfs";
  public static final String MAPRFS_SCHEME = "maprfs";
  public static final String WASB_SCHEME = "wasb";
  public static final String INDENT = "  ";
  public static final String ROOT_INDENT = "    ";
  private static final Logger LOGGER = LoggerFactory.getLogger( NamedClusterImpl.class );

  private VariableSpace variables = new Variables();

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
  @MetaStoreAttribute( password = true )
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
  @Deprecated
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
  private long lastModifiedDate = System.currentTimeMillis();

  private ITwoWayPasswordEncoder passwordEncoder = new Base64TwoWayPasswordEncoder();

  public NamedClusterImpl() {
    initializeVariablesFrom( null );
  }

  public NamedClusterImpl( NamedCluster namedCluster ) {
    this();
    replaceMeta( namedCluster );
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public String getShimIdentifier() {
    return shimIdentifier;
  }

  public void setShimIdentifier( String shimIdentifier ) {
    this.shimIdentifier = shimIdentifier;
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
    this.setGatewayUrl( nc.getGatewayUrl() );
    this.setGatewayUsername( nc.getGatewayUsername() );
    this.setGatewayPassword( nc.getGatewayPassword() );
    this.setUseGateway( nc.isUseGateway() );
    this.lastModifiedDate = System.currentTimeMillis();
  }

  public NamedClusterImpl clone() {
    return new NamedClusterImpl( this );
  }

  @Override
  public String processURLsubstitution( String incomingURL, IMetaStore metastore, VariableSpace variableSpace ) {
    StringBuilder outgoingUrl = new StringBuilder();
    //if we have some of variable inside the incoming url will substitute it with value
    if( variableSpace != null ) {
      variableSpace.initializeVariablesFrom( getParentVariableSpace() );
      HashMap<String, String> varaibles = new HashMap<>();
      List<String> varName = new ArrayList<String>();
      StringUtil.getUsedVariables( incomingURL, varName, true );
      varName.stream().forEach(  item -> varaibles.put( item, variableSpace.getVariable( item ) )  );
      incomingURL = StringUtil.environmentSubstitute( incomingURL, varaibles );
    }
    //if we do not get something like nc://clusterName or hdfs|wasb|maprfs://hostname  or at first place we found open variable tag
    //we need to add /
    if ( !incomingURL.startsWith( HDFS_SCHEME )
        & !incomingURL.startsWith( WASB_SCHEME )
        & !incomingURL.startsWith( MAPRFS_SCHEME )
        & !incomingURL.startsWith( "nc" )
        & !incomingURL.startsWith( StringUtil.FIELD_OPEN )
        & !incomingURL.startsWith( StringUtil.HEX_OPEN )
        & !incomingURL.startsWith( StringUtil.WINDOWS_OPEN ) 
        & !incomingURL.startsWith( StringUtil.UNIX_OPEN ) ) {
      incomingURL = incomingURL.startsWith( "/" ) ? incomingURL : "/" + incomingURL;
    }
    // we will return nc://path1/path2 or nc://${variableName} or nc://hdfs:// which allow to use expected file providers
    return outgoingUrl.append( "nc" ).append( "://" ).append( getName() ).append( incomingURL ).toString();
  }

  public String getHostNameParsed( VariableSpace variableSpace ) {
    if ( StringUtil.isVariable( hdfsHost ) ) {
      if ( variableSpace == null ) {
        return null;
      }
      return variableSpace.getVariable( StringUtil.getVariableName( getHdfsHost() ) );
    }
    return hdfsHost != null ? hdfsHost.trim() : null;
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
      if ( other.getName() != null ) {
        return false;
      }
    } else if ( !name.equals( other.getName() ) ) {
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

  @Override
  public String toString() {
    return "Named cluster: " + getName();
  }

  public String toXmlForEmbed( String rootTag ) {
    BeanMap m = new BeanMap( this );
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = null;
    Document doc = null;
    try {
      builder = dbf.newDocumentBuilder();
      doc = builder.newDocument();
      Element rootNode = doc.createElement( rootTag );
      doc.appendChild( rootNode );
      Iterator<Map.Entry<Object, Object>> i = m.entryIterator();
      while ( i.hasNext() ) {
        Map.Entry<Object, Object> entry = i.next();
        String elementName = (String) entry.getKey();
        if ( !"class".equals( elementName ) && !"parentVariableSpace".equals( elementName ) ) {
          String value = "";
          String type = "String";
          Object o = entry.getValue();
          if ( o != null ) {
            if ( o instanceof Long ) {
              value = Long.toString( (Long) o );
            } else if ( o instanceof Boolean ) {
              value = Boolean.toString( (Boolean) o );
            } else {
              try {
                value = (String) entry.getValue();
                if ( elementName.toLowerCase().contains( "password" ) ) {
                  value = passwordEncoder.encode( value );
                }
              } catch ( Exception e ) {
                e.printStackTrace();
              }
            }
          }
          rootNode.appendChild( createChildElement( doc, elementName, type, value ) );
        }
      }
      DOMSource domSource = new DOMSource( doc );
      StringWriter writer = new StringWriter();
      StreamResult result = new StreamResult( writer );
      TransformerFactory tf = TransformerFactory.newInstance();
      Transformer transformer = tf.newTransformer();
      transformer.transform( domSource, result );
      String s = writer.toString();
      // Remove header from the XML
      s = s.substring( s.indexOf( ">" ) + 1 );
      return s;
    } catch ( ParserConfigurationException | TransformerException e1 ) {
      LOGGER.error( "Could not parse embedded cluster xml" + e1.toString() );
      return "";
    }
  }

  public NamedCluster fromXmlForEmbed( Node node ) {
    NamedClusterImpl returnCluster = this.clone();
    List<Node> fields = XMLHandler.getNodes( node, "child" );
    for ( Node field: fields ) {
      String fieldName = XMLHandler.getTagValue( field, "id" );
      String fieldValue = XMLHandler.getTagValue(  field, "value" );
      if ( fieldName.toLowerCase().contains( "password" ) ) {
        fieldValue = passwordEncoder.decode( fieldValue );
      }
      try {
        BeanUtils.setProperty( returnCluster, fieldName, fieldValue );
      } catch ( IllegalAccessException | InvocationTargetException e ) {
        LOGGER.error( "Could not serialize NamedCluster to xml: " + e.toString() );
      }
    }
    return returnCluster;
  }

  private Node createChildElement( Document doc, String elementName, String elementType, String elementValue ) {
    Element childNode = doc.createElement( "child" );
    childNode.appendChild( createTextNode( doc, "id", elementName ) );
    childNode.appendChild( createTextNode( doc, "value", elementValue ) );
    childNode.appendChild( createTextNode( doc, "type", elementType ) );
    return childNode;
  }

  private Node createTextNode( Document doc, String tagName, String value ) {
    Node node = doc.createElement( tagName );
    node.appendChild( doc.createTextNode( value ) );
    return node;
  }

  @Override
  public String getGatewayUrl() {
    return gatewayUrl;
  }

  @Override
  public void setGatewayUrl( String gatewayUrl ) {
    this.gatewayUrl = gatewayUrl;
  }

  @Override
  public String getGatewayUsername() {
    return gatewayUsername;
  }

  @Override
  public void setGatewayUsername( String gatewayUsername ) {
    this.gatewayUsername = gatewayUsername;
  }

  @Override
  public String getGatewayPassword() {
    return gatewayPassword;
  }

  @Override
  public void setGatewayPassword( String gatewayPassword ) {
    this.gatewayPassword = gatewayPassword;
  }

  @Override
  public boolean isUseGateway() {
    return useGateway;
  }

  @Override
  public void setUseGateway( boolean useGateway ) {
    this.useGateway = useGateway;
  }

}
