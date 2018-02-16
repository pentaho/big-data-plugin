/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

import com.google.common.collect.Maps;
import org.pentaho.di.core.ObjectLocationSpecificationMethod;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.streaming.common.BaseStreamStepMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.toList;
import static org.pentaho.di.trans.step.mqtt.MQTTClientBuilder.DEFAULT_SSL_OPTS;

@Step ( id = "MQTTConsumer", image = "MQTTConsumer.svg",
  i18nPackageName = "org.pentaho.di.trans.step.mqtt",
  name = "MQTTConsumer.TypeLongDesc",
  description = "MQTTConsumer.TypeTooltipDesc",
  categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Streaming" )
@InjectionSupported ( localizationPrefix = "MQTTConsumerMeta.Injection.", groups = { "SSL" } )
public class MQTTConsumerMeta extends BaseStreamStepMeta implements StepMetaInterface, Cloneable {

  public static final String MQTT_SERVER = "MQTT_SERVER";
  public static final String TOPICS = "TOPICS";
  public static final String MSG_OUTPUT_NAME = "MSG_OUTPUT_NAME";
  public static final String TOPIC_OUTPUT_NAME = "TOPIC_OUTPUT_NAME";
  public static final String QOS = "QOS";
  public static final String SSL_GROUP = "SSL";
  public static final String USE_SSL = "USE_SSL";
  public static final String SSL_KEYS = "SSL_KEYS";
  public static final String SSL_VALUES = "SSL_VALUES";
  public static final String USERNAME = "USERNAME";
  private static Class<?> PKG = MQTTConsumer.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$


  @Injection ( name = MQTT_SERVER )
  public String mqttServer = "";

  @Injection ( name = TOPICS )
  public List<String> topics = new ArrayList<>();

  @Injection ( name = MSG_OUTPUT_NAME )
  public String msgOutputName = "Message";

  @Injection ( name = TOPIC_OUTPUT_NAME )
  public String topicOutputName = "Topic";

  @Injection ( name = QOS )
  public String qos = "0";

  @Injection ( name = USERNAME )
  public String username = "";

  @Injection ( name = PASSWORD )
  public String password = "";

  @Injection ( name = USE_SSL, group = SSL_GROUP )
  public Boolean useSsl = new Boolean( false );

  @Injection ( name = SSL_KEYS, group = SSL_GROUP )
  public List<String> sslKeys = new ArrayList();

  @Injection ( name = SSL_VALUES, group = SSL_GROUP )
  public List<String> sslValues = new ArrayList();

  public MQTTConsumerMeta() {
    super();

    setSpecificationMethod( ObjectLocationSpecificationMethod.FILENAME );
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  public void setDefault() {
    super.setDefault();
    mqttServer = "";
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

  @Override public String getFileName() {
    return getTransformationPath();
  }

  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    super.readRep( rep, metaStore, id_step, databases );
    setMqttServer( rep.getStepAttributeString( id_step, MQTT_SERVER ) );
    setMsgOutputName( rep.getStepAttributeString( id_step, MSG_OUTPUT_NAME ) );
    setTopicOutputName( rep.getStepAttributeString( id_step, TOPIC_OUTPUT_NAME ) );
    int topicCount = rep.countNrStepAttributes( id_step, TOPICS );
    for ( int i = 0; i < topicCount; i++ ) {
      topics.add( rep.getStepAttributeString( id_step, i, TOPICS ) );
    }
    setQos( rep.getStepAttributeString( id_step, QOS ) );
    setUsername( rep.getStepAttributeString( id_step, USERNAME ) );
    setPassword( Encr.decryptPasswordOptionallyEncrypted( rep.getStepAttributeString( id_step, PASSWORD ) ) );

    setUseSsl( rep.getStepAttributeBoolean( id_step, USE_SSL ) );

    int sslKeyCount = rep.countNrStepAttributes( id_step, SSL_KEYS );
    int sslValCount = rep.countNrStepAttributes( id_step, SSL_VALUES );

    checkState( sslKeyCount == sslValCount );
    sslKeys.clear();
    sslValues.clear();
    for ( int j = 0; j < sslKeyCount; j++ ) {
      String key = rep.getStepAttributeString( id_step, j, SSL_KEYS );
      sslKeys.add( key );
      String value = rep.getStepAttributeString( id_step, j, SSL_VALUES );
      sslValues.add( maybeDecrypt( key, value ) );
    }
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId transId, ObjectId stepId )
    throws KettleException {
    super.saveRep( rep, metaStore, transId, stepId );
    rep.saveStepAttribute( transId, stepId, MQTT_SERVER, mqttServer );
    rep.saveStepAttribute( transId, stepId, MSG_OUTPUT_NAME, msgOutputName );
    rep.saveStepAttribute( transId, stepId, TOPIC_OUTPUT_NAME, topicOutputName );
    int i = 0;
    for ( String topic : topics ) {
      rep.saveStepAttribute( transId, stepId, i++, TOPICS, topic );
    }
    rep.saveStepAttribute( transId, stepId, QOS, qos );
    rep.saveStepAttribute( transId, stepId, USERNAME, username );
    rep.saveStepAttribute( transId, stepId, PASSWORD, Encr.encryptPasswordIfNotUsingVariables( password ) );

    rep.saveStepAttribute( transId, stepId, USE_SSL, isUseSsl() );
    checkState( sslKeys.size() == sslValues.size() );
    for ( int j = 0; j < sslKeys.size(); j++ ) {
      rep.saveStepAttribute( transId, stepId, SSL_KEYS, sslKeys.get( j ) );
      rep.saveStepAttribute( transId, stepId, SSL_VALUES, maybeEncrypt( sslKeys.get( j ), sslValues.get( j ) ) );
    }
  }

  private String maybeEncrypt( String keyName, String value ) {
    return isSensitive( keyName )
      ? Encr.encryptPasswordIfNotUsingVariables( value ) : value;
  }

  private String maybeDecrypt( String keyName, String value ) {
    return isSensitive( keyName )
      ? Encr.decryptPasswordOptionallyEncrypted( value ) : value;
  }

  private boolean isSensitive( String keyName ) {
    return keyName.toUpperCase().contains( PASSWORD );
  }


  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {
    rowMeta.addValueMeta( new ValueMetaString( msgOutputName ) );
    rowMeta.addValueMeta( new ValueMetaString( topicOutputName ) );
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
                                Trans trans ) {
    return new MQTTConsumer( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  public StepDataInterface getStepData() {
    return new MQTTConsumerData();
  }

  public String getDialogClassName() {
    return "org.pentaho.di.trans.step.mqtt.MQTTConsumerDialog";
  }

  public String getMqttServer() {
    return mqttServer;
  }

  public void setMqttServer( String mqttServer ) {
    this.mqttServer = mqttServer;
  }

  public List<String> getTopics() {
    return topics;
  }

  public void setTopics( List<String> topics ) {
    this.topics = topics;
  }

  public String getMsgOutputName() {
    return msgOutputName;
  }

  public void setMsgOutputName( String msgOutputName ) {
    this.msgOutputName = msgOutputName;
  }

  public String getTopicOutputName() {
    return topicOutputName;
  }

  public void setTopicOutputName( String topicOutputName ) {
    this.topicOutputName = topicOutputName;
  }

  public String getQos() {
    return qos;
  }

  public void setQos( String qos ) {
    this.qos = qos;
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
    checkState( sslKeys != null
      && sslValues != null
      && sslKeys.size() == sslValues.size() );
    return Maps.toMap( sslKeys, key -> sslValues.get( sslKeys.indexOf( key ) ) );
  }

  public void setSslConfig( Map<String, String> sslConfig ) {
    checkNotNull( sslConfig );
    sslKeys = newArrayList( sslConfig.keySet() );
    sslValues = sslKeys.stream()
      .map( key -> sslConfig.get( key ) )
      .collect( toList() );
  }

  public boolean isUseSsl() {
    return useSsl;
  }

  public void setUseSsl( boolean useSsl ) {
    this.useSsl = useSsl;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    MQTTConsumerMeta that = (MQTTConsumerMeta) o;
    return Objects.equals( useSsl, that.useSsl )
      && Objects.equals( mqttServer, that.mqttServer )
      && Objects.equals( topics, that.topics )
      && Objects.equals( msgOutputName, that.msgOutputName )
      && Objects.equals( topicOutputName, that.topicOutputName )
      && Objects.equals( qos, that.qos )
      && Objects.equals( sslKeys, that.sslKeys )
      && Objects.equals( sslValues, that.sslValues )
      && Objects.equals( username, that.username )
      && Objects.equals( password, that.password );
  }

  @Override public int hashCode() {

    return Objects
      .hash( mqttServer, topics, msgOutputName, topicOutputName, qos, useSsl, sslKeys, sslValues, username, password );
  }
}
