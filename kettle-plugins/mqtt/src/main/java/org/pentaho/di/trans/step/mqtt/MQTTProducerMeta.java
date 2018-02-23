/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.step.mqtt;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
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
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static org.pentaho.di.trans.step.mqtt.MQTTClientBuilder.DEFAULT_SSL_OPTS;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.CLIENT_ID;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.MESSAGE_FIELD;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.MQTT_SERVER;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.PASSWORD;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.QOS;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.SSL_GROUP;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.SSL_KEYS;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.SSL_VALUES;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.TOPIC;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.USERNAME;
import static org.pentaho.di.trans.step.mqtt.MQTTConstants.USE_SSL;
import static org.pentaho.di.trans.step.mqtt.SslConfigHelper.conf;

@Step ( id = "MQTTProducer", image = "MQTTProducer.svg",
  i18nPackageName = "org.pentaho.di.trans.step.mqtt",
  name = "MQTTProducer.TypeLongDesc",
  description = "MQTTProducer.TypeTooltipDesc",
  categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Streaming" )
@InjectionSupported ( localizationPrefix = "MQTTProducerMeta.Injection." )
public class MQTTProducerMeta extends BaseStepMeta implements StepMetaInterface {

  @Injection ( name = MQTT_SERVER )
  private String mqttServer;

  @Injection ( name = CLIENT_ID )
  private String clientId;

  @Injection ( name = TOPIC )
  private String topic;

  @Injection ( name = QOS )
  private String qos;

  @Injection ( name = MESSAGE_FIELD )
  private String messageField;

  @Injection ( name = USERNAME )
  private String username;

  @Injection ( name = PASSWORD )
  private String password;

  @Injection ( name = USE_SSL, group = SSL_GROUP )
  private Boolean useSsl = false;

  @Injection ( name = SSL_KEYS, group = SSL_GROUP )
  private List<String> sslKeys = new ArrayList<>();

  @Injection ( name = SSL_VALUES, group = SSL_GROUP )
  private List<String> sslValues = new ArrayList<>();


  public MQTTProducerMeta() {
    super();
  }

  @Override
  public void setDefault() {
    mqttServer = "";
    topic = "";
    qos = "0";
    username = "";
    password = "";

    sslKeys = DEFAULT_SSL_OPTS
      .keySet().stream()
      .sorted()
      .collect( toList() );
    sslValues = sslKeys.stream()
      .map( DEFAULT_SSL_OPTS::get )
      .collect( toList() );
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                                Trans trans ) {
    return new MQTTProducer( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new MQTTProducerData();
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases )
    throws KettleException {
    super.readRep( rep, metaStore, stepId, databases );
    setMqttServer( rep.getStepAttributeString( stepId, MQTT_SERVER ) );
    setClientId( rep.getStepAttributeString( stepId, CLIENT_ID ) );
    setTopic( rep.getStepAttributeString( stepId, TOPIC ) );
    setQOS( rep.getStepAttributeString( stepId, QOS ) );
    setMessageField( rep.getStepAttributeString( stepId, MESSAGE_FIELD ) );
    setUsername( rep.getStepAttributeString( stepId, USERNAME ) );
    setPassword( Encr.decryptPasswordOptionallyEncrypted( rep.getStepAttributeString( stepId, PASSWORD ) ) );
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId transformationId, ObjectId stepId )
    throws KettleException {
    super.saveRep( rep, metaStore, transformationId, stepId );
    rep.saveStepAttribute( transformationId, stepId, MQTT_SERVER, mqttServer );
    rep.saveStepAttribute( transformationId, stepId, CLIENT_ID, clientId );
    rep.saveStepAttribute( transformationId, stepId, TOPIC, topic );
    rep.saveStepAttribute( transformationId, stepId, QOS, qos );
    rep.saveStepAttribute( transformationId, stepId, MESSAGE_FIELD, messageField );
    rep.saveStepAttribute( transformationId, stepId, USERNAME, username );
    rep.saveStepAttribute( transformationId, stepId, PASSWORD, Encr.encryptPasswordIfNotUsingVariables( password ) );
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder();
    retval.append( "    " ).append( XMLHandler.addTagValue( MQTT_SERVER, mqttServer ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( CLIENT_ID, clientId ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( TOPIC, topic ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( QOS, qos ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( MESSAGE_FIELD, messageField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( USERNAME, username ) );
    retval.append( "    " )
      .append( XMLHandler.addTagValue( PASSWORD, Encr.encryptPasswordIfNotUsingVariables( password ) ) );

    retval.append( "<SSL>\n" );
    sslKeys.forEach( key -> retval.append( "<SSL_KEYS>" ).append( key ).append( "</SSL_KEYS>\n" ) );
    sslValues.forEach( val -> retval.append( "<SSL_VALUES>" ).append( val ).append( "</SSL_VALUES>\n" ) );
    retval.append( "<USE_SSL>" ).append( isUseSsl() ).append( "</USE_SSL>\n" );
    retval.append( "</SSL>\n" );

    return retval.toString();
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) {
    readData( stepnode );
  }

  private void readData( Node stepnode ) {
    setMqttServer( XMLHandler.getTagValue( stepnode, MQTT_SERVER ) );
    setClientId( XMLHandler.getTagValue( stepnode, CLIENT_ID ) );
    setTopic( XMLHandler.getTagValue( stepnode, TOPIC ) );
    setQOS( XMLHandler.getTagValue( stepnode, QOS ) );
    setMessageField( XMLHandler.getTagValue( stepnode, MESSAGE_FIELD ) );
    setUsername( XMLHandler.getTagValue( stepnode, USERNAME ) );
    setPassword( Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( stepnode, PASSWORD ) ) );


    Node sslNode = XMLHandler.getSubNode( stepnode, "SSL" );
    List<Node> sslKeyNodes = XMLHandler.getNodes( sslNode, "SSL_KEYS" );
    List<Node> sslValNodes = XMLHandler.getNodes( sslNode, "SSL_VALUES" );
    String useSslNode = XMLHandler.getTagValue( sslNode, "USE_SSL" );

    sslKeys = new ArrayList<>();
    sslValues = new ArrayList<>();
    ofNullable( sslKeyNodes ).orElse( emptyList() ).stream()
      .map( Node::getTextContent )
      .forEach( sslKeys::add );
    ofNullable( sslValNodes ).orElse( emptyList() ).stream()
      .map( Node::getTextContent )
      .forEach( sslValues::add );
    useSsl = Boolean.parseBoolean( useSslNode );
  }

  @SuppressWarnings ( { "deprecation" } )
  // can be removed once the new @StepDialog annotation supports OSGi
  @Override
  public String getDialogClassName() {
    return "org.pentaho.di.trans.step.mqtt.MQTTProducerDialog";
  }

  public String getMqttServer() {
    return mqttServer;
  }

  public void setMqttServer( String mqttServer ) {
    this.mqttServer = mqttServer;
  }

  public String getClientId() {
    return clientId;
  }

  public void setClientId( String clientId ) {
    this.clientId = clientId;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic( String topic ) {
    this.topic = topic;
  }

  public String getQOS() {
    return qos;
  }

  public void setQOS( String qos ) {
    this.qos = qos;
  }

  public String getMessageField() {
    return messageField;
  }

  public void setMessageField( String messageField ) {
    this.messageField = messageField;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername( String username ) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword( String password ) {
    this.password = password;
  }

  public Map<String, String> getSslConfig() {
    return conf( sslKeys, sslValues ).asMap();
  }

  public void setSslConfig( Map<String, String> sslConfig ) {
    sslKeys = conf( sslConfig ).keys();
    sslValues = conf( sslConfig ).vals();
  }

  public boolean isUseSsl() {
    return useSsl;
  }

  public void setUseSsl( boolean useSsl ) {
    this.useSsl = useSsl;
  }


}
