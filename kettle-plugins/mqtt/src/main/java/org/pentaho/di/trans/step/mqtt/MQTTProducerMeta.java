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
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
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

import java.util.List;

@Step( id = "MQTTProducer", image = "MQTTProducer.svg",
  i18nPackageName = "org.pentaho.di.trans.step.mqtt",
  name = "MQTTProducer.TypeLongDesc",
  description = "MQTTProducer.TypeTooltipDesc",
  categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Streaming" )
@InjectionSupported( localizationPrefix = "MQTTProducerMeta.Injection." )
public class MQTTProducerMeta extends BaseStepMeta implements StepMetaInterface, Cloneable {

  public static final String MQTT_SERVER = "MQTT_SERVER";
  public static final String CLIENT_ID = "CLIENT_ID";
  public static final String TOPIC = "TOPIC";
  public static final String QOS = "QOS";
  public static final String MESSAGE_FIELD = "MESSAGE_FIELD";

  private static Class<?> PKG = MQTTProducer.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  @Injection( name = MQTT_SERVER )
  private String mqttServer;

  @Injection( name = CLIENT_ID )
  private String clientId;

  @Injection( name = TOPIC )
  private String topic;

  @Injection( name = QOS )
  private String qos;

  @Injection( name = MESSAGE_FIELD )
  private String messageField;

  public MQTTProducerMeta() {
    super();
  }

  @Override
  public void setDefault() {
    mqttServer = "";
    topic = "";
    qos = "0";
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
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder();
    retval.append( "    " ).append( XMLHandler.addTagValue( MQTT_SERVER, mqttServer ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( CLIENT_ID, clientId ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( TOPIC, topic ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( QOS, qos ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( MESSAGE_FIELD, messageField ) );

    return retval.toString();
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode );
  }

  private void readData( Node stepnode ) {
    setMqttServer( XMLHandler.getTagValue( stepnode, MQTT_SERVER ) );
    setClientId( XMLHandler.getTagValue( stepnode, CLIENT_ID ) );
    setTopic( XMLHandler.getTagValue( stepnode, TOPIC ) );
    setQOS( XMLHandler.getTagValue( stepnode, QOS ) );
    setMessageField( XMLHandler.getTagValue( stepnode, MESSAGE_FIELD ) );
  }

  @Override
  public String getDialogClassName() {
    return "org.pentaho.di.trans.step.mqtt.MQTTProducerDialog";
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
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
}
