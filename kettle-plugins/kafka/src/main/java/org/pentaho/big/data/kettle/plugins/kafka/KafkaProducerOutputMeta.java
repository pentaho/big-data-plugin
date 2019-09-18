/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.pentaho.big.data.kettle.plugins.kafka;

import com.google.common.base.Preconditions;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.hadoop.shim.api.jaas.JaasConfigService;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;
import org.w3c.dom.Node;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.pentaho.big.data.kettle.plugins.kafka.KafkaProducerOutputMeta.ConnectionType.DIRECT;

@Step( id = "KafkaProducerOutput", image = "KafkaProducerOutput.svg",
  i18nPackageName = "org.pentaho.big.data.kettle.plugins.kafka",
  name = "KafkaProducer.TypeLongDesc",
  description = "KafkaProducer.TypeTooltipDesc",
  categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Streaming",
  documentationUrl = "Products/Kafka_Producer" )
@InjectionSupported( localizationPrefix = "KafkaProducerOutputMeta.Injection.", groups = { "CONFIGURATION_PROPERTIES" } )
public class KafkaProducerOutputMeta extends BaseStepMeta implements StepMetaInterface {
  public enum ConnectionType {
    DIRECT,
    CLUSTER
  }

  public static final String CLUSTER_NAME = "clusterName";
  public static final String CONNECTION_TYPE = "connectionType";
  public static final String DIRECT_BOOTSTRAP_SERVERS = "directBootstrapServers";
  public static final String CLIENT_ID = "clientId";
  public static final String TOPIC = "topic";
  public static final String KEY_FIELD = "keyField";
  public static final String MESSAGE_FIELD = "messageField";
  public static final String ADVANCED_CONFIG = "advancedConfig";
  public static final String CONFIG_OPTION = "option";
  public static final String OPTION_PROPERTY = "property";
  public static final String OPTION_VALUE = "value";

  @Injection( name = "CONNECTION_TYPE" )
  private ConnectionType connectionType = DIRECT;

  @Injection( name = "DIRECT_BOOTSTRAP_SERVERS" )
  private String directBootstrapServers;

  @Injection( name = "CLUSTER_NAME" )
  private String clusterName;

  @Injection( name = "CLIENT_ID" )
  private String clientId;

  @Injection( name = "TOPIC" )
  private String topicVal;

  @Injection( name = "KEY_FIELD" )
  private String keyField;

  @Injection( name = "MESSAGE_FIELD" )
  private String messageField;

  @Injection( name = "NAMES", group = "CONFIGURATION_PROPERTIES" )
  protected List<String> injectedConfigNames;

  @Injection( name = "VALUES", group = "CONFIGURATION_PROPERTIES" )
  protected List<String> injectedConfigValues;

  private Map<String, String> config = new LinkedHashMap<>();

  private NamedClusterService namedClusterService;

  private NamedClusterServiceLocator namedClusterServiceLocator;

  private MetastoreLocator metastoreLocator;

  public KafkaProducerOutputMeta() {
    super();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) {
    readData( stepnode );
  }

  private void readData( Node stepnode ) {
    setConnectionType( ConnectionType.valueOf( XMLHandler.getTagValue( stepnode, CONNECTION_TYPE ) ) );
    setDirectBootstrapServers( XMLHandler.getTagValue( stepnode, DIRECT_BOOTSTRAP_SERVERS ) );
    setClusterName( XMLHandler.getTagValue( stepnode, CLUSTER_NAME ) );
    setClientId( XMLHandler.getTagValue( stepnode, CLIENT_ID ) );
    setTopic( XMLHandler.getTagValue( stepnode, TOPIC ) );
    setKeyField( XMLHandler.getTagValue( stepnode, KEY_FIELD ) );
    setMessageField( XMLHandler.getTagValue( stepnode, MESSAGE_FIELD ) );

    config = new LinkedHashMap<>();

    Optional.ofNullable( XMLHandler.getSubNode( stepnode, ADVANCED_CONFIG ) ).map( Node::getChildNodes )
        .ifPresent( nodes -> IntStream.range( 0, nodes.getLength() ).mapToObj( nodes::item )
            .filter( node -> node.getNodeType() == Node.ELEMENT_NODE )
            .forEach( node -> {
              if ( CONFIG_OPTION.equals( node.getNodeName() ) ) {
                config.put( node.getAttributes().getNamedItem( OPTION_PROPERTY ).getTextContent(),
                  node.getAttributes().getNamedItem( OPTION_VALUE ).getTextContent() );
              } else {
                config.put( node.getNodeName(), node.getTextContent() );
              }
            } ) );
  }

  @Override public void setDefault() {
    // no defaults
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases )
    throws KettleException {
    setConnectionType( ConnectionType.valueOf( rep.getStepAttributeString( stepId, CONNECTION_TYPE ) ) );
    setDirectBootstrapServers( rep.getStepAttributeString( stepId, DIRECT_BOOTSTRAP_SERVERS ) );
    setClusterName( rep.getStepAttributeString( stepId, CLUSTER_NAME ) );
    setClientId( rep.getStepAttributeString( stepId, CLIENT_ID ) );
    setTopic( rep.getStepAttributeString( stepId, TOPIC ) );
    setKeyField( rep.getStepAttributeString( stepId, KEY_FIELD ) );
    setMessageField( rep.getStepAttributeString( stepId, MESSAGE_FIELD ) );

    config = new LinkedHashMap<>();

    for ( int i = 0; i < rep.getStepAttributeInteger( stepId, ADVANCED_CONFIG + "_COUNT" ); i++ ) {
      config.put( rep.getStepAttributeString( stepId, i, ADVANCED_CONFIG + "_NAME" ),
          rep.getStepAttributeString( stepId, i, ADVANCED_CONFIG + "_VALUE" ) );
    }
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId transformationId, ObjectId stepId )
    throws KettleException {
    rep.saveStepAttribute( transformationId, stepId, CONNECTION_TYPE, connectionType.name() );
    rep.saveStepAttribute( transformationId, stepId, DIRECT_BOOTSTRAP_SERVERS, directBootstrapServers );
    rep.saveStepAttribute( transformationId, stepId, CLUSTER_NAME, clusterName );
    rep.saveStepAttribute( transformationId, stepId, CLIENT_ID, clientId );
    rep.saveStepAttribute( transformationId, stepId, TOPIC, topicVal );
    rep.saveStepAttribute( transformationId, stepId, KEY_FIELD, keyField );
    rep.saveStepAttribute( transformationId, stepId, MESSAGE_FIELD, messageField );

    rep.saveStepAttribute( transformationId, stepId, ADVANCED_CONFIG + "_COUNT", getConfig().size() );

    int i = 0;
    for ( String propName : getConfig().keySet() ) {
      rep.saveStepAttribute( transformationId, stepId, i, ADVANCED_CONFIG + "_NAME", propName );
      rep.saveStepAttribute( transformationId, stepId, i++, ADVANCED_CONFIG + "_VALUE", getConfig().get( propName ) );
    }
  }

  @Override public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, Repository repository, IMetaStore metaStore ) {
    // Default: nothing changes to rowMeta
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
                                Trans trans ) {
    return new KafkaProducerOutput( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new KafkaProducerOutputData();
  }

  @SuppressWarnings( "deprecation" )
  public String getDialogClassName() {
    return "org.pentaho.big.data.kettle.plugins.kafka.KafkaProducerOutputDialog";
  }

  public String getBootstrapServers() {
    if ( DIRECT.equals( getConnectionType() ) ) {
      return getDirectBootstrapServers();
    }

    IMetaStore metastore = metastoreLocator.getMetastore();
    if ( metastore == null ) {
      //todo: this needed for spark.  should make metastoreLocator know how to find embedded metastore in spark
      metastore = getParentStepMeta().getParentTransMeta().getEmbeddedMetaStore();
    }
    Optional<NamedCluster> namedClusterByName = Optional.ofNullable(
      namedClusterService.getNamedClusterByName(
        parentStepMeta.getParentTransMeta().environmentSubstitute( clusterName ), metastore ) );
    if ( !namedClusterByName.isPresent() ) {
      namedClusterByName = Optional.ofNullable(
        namedClusterService.getNamedClusterByName(
          parentStepMeta.getParentTransMeta().environmentSubstitute( clusterName ),
          getParentStepMeta().getParentTransMeta().getEmbeddedMetaStore() ) );
    }
    return namedClusterByName
        .map( NamedCluster::getKafkaBootstrapServers ).orElse( "" );
  }

  public String getClientId() {
    return clientId;
  }

  public void setClientId( String clientId ) {
    this.clientId = clientId;
  }

  public String getTopic() {
    return topicVal;
  }

  public void setTopic( String topic ) {
    this.topicVal = topic;
  }

  public String getKeyField() {
    return keyField;
  }

  public void setKeyField( String keyField ) {
    this.keyField = keyField;
  }

  public String getMessageField() {
    return messageField;
  }

  public void setMessageField( String messageField ) {
    this.messageField = messageField;
  }

  public ConnectionType getConnectionType() {
    return connectionType;
  }

  public void setConnectionType( final ConnectionType connectionType ) {
    this.connectionType = connectionType;
  }

  @Override public String getXML() {
    StringBuilder retval = new StringBuilder();
    retval.append( "    " ).append( XMLHandler.addTagValue( CONNECTION_TYPE, connectionType.name() ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( DIRECT_BOOTSTRAP_SERVERS, directBootstrapServers ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( CLUSTER_NAME, clusterName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( TOPIC, topicVal ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( CLIENT_ID, clientId ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( KEY_FIELD, keyField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( MESSAGE_FIELD, messageField ) );
    retval.append( "    " ).append( XMLHandler.openTag( ADVANCED_CONFIG ) ).append( Const.CR );
    getConfig().forEach( ( key, value ) -> retval.append( "        " )
      .append( XMLHandler.addTagValue( CONFIG_OPTION, "", true,
        OPTION_PROPERTY, (String) key, OPTION_VALUE, (String) value ) ) );
    retval.append( "    " ).append( XMLHandler.closeTag( ADVANCED_CONFIG ) ).append( Const.CR );

    parentStepMeta.getParentTransMeta().getNamedClusterEmbedManager().registerUrl( "hc://" + clusterName );
    return retval.toString();
  }

  public NamedClusterService getNamedClusterService() {
    return namedClusterService;
  }

  public MetastoreLocator getMetastoreLocator() {
    return metastoreLocator;
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName( String clusterName ) {
    this.clusterName = clusterName;
  }

  public void setDirectBootstrapServers( final String directBootstrapServers ) {
    this.directBootstrapServers = directBootstrapServers;
  }

  public String getDirectBootstrapServers() {
    return directBootstrapServers;
  }

  public void setConfig( Map<String, String> config ) {
    this.config = config;
  }

  public Map<String, String> getConfig() {
    applyInjectedProperties();
    return config;
  }

  public void setNamedClusterService( NamedClusterService namedClusterService ) {
    this.namedClusterService = namedClusterService;
  }

  public void setMetastoreLocator( MetastoreLocator metastoreLocator ) {
    this.metastoreLocator = metastoreLocator;
  }

  public NamedClusterServiceLocator getNamedClusterServiceLocator() {
    return namedClusterServiceLocator;
  }

  public void setNamedClusterServiceLocator(
    NamedClusterServiceLocator namedClusterServiceLocator ) {
    this.namedClusterServiceLocator = namedClusterServiceLocator;
  }

  public Optional<JaasConfigService> getJaasConfigService() {
    try {
      return Optional.ofNullable( namedClusterServiceLocator.getService(
        namedClusterService.getNamedClusterByName( getClusterName(), getMetastoreLocator().getMetastore() ),
        JaasConfigService.class ) );
    } catch ( Exception e ) {
      getLog().logDebug( "problem getting jaas config", e );
      return Optional.empty();
    }
  }

  protected void applyInjectedProperties() {
    if ( injectedConfigNames != null || injectedConfigValues != null ) {
      Preconditions.checkState( injectedConfigNames != null, "Options names were not injected" );
      Preconditions.checkState( injectedConfigValues != null, "Options values were not injected" );
      Preconditions.checkState( injectedConfigNames.size() == injectedConfigValues.size(),
          "Injected different number of options names and value" );

      setConfig( IntStream.range( 0, injectedConfigNames.size() ).boxed().collect( Collectors
          .toMap( injectedConfigNames::get, injectedConfigValues::get, ( v1, v2 ) -> v1,
              LinkedHashMap::new ) ) );

      injectedConfigNames = null;
      injectedConfigValues = null;
    }
  }
}
