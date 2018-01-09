/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

import org.pentaho.di.core.ObjectLocationSpecificationMethod;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
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


@Step( id = "MQTTConsumer", image = "MQTTConsumer.svg", name = "MQTT Consumer",
  description = "Subscribes and streams an MQTT Topic", categoryDescription = "Streaming" )
@InjectionSupported( localizationPrefix = "MQTTConsumerMeta.Injection." )
public class MQTTConsumerMeta extends BaseStreamStepMeta implements StepMetaInterface, Cloneable {

  public static final String MQTT_SERVER = "MQTT_SERVER";
  public static final String TOPICS = "TOPICS";
  public static final String MSG_OUTPUT_NAME = "MSG_OUTPUT_NAME";
  public static final String TOPIC_OUTPUT_NAME = "TOPIC_OUTPUT_NAME";
  private static Class<?> PKG = MQTTConsumer.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  @Injection( name = MQTT_SERVER )
  public String mqttServer = "";

  @Injection( name = TOPICS )
  public List<String> topics = new ArrayList<>();

  @Injection( name = MSG_OUTPUT_NAME )
  public String msgOutputName = "Message";

  @Injection( name = TOPIC_OUTPUT_NAME )
  public String topicOutputName = "Topic";

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
  }

  @Override public String getFileName() {
    return getTransformationPath();
  }

  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    super.readRep( rep, metaStore, id_step, databases );
    setMqttServer( rep.getStepAttributeString( id_step, MQTT_SERVER  ) );
    setMsgOutputName( rep.getStepAttributeString( id_step, MSG_OUTPUT_NAME  ) );
    setTopicOutputName( rep.getStepAttributeString( id_step, TOPIC_OUTPUT_NAME  ) );
    int topicCount = rep.countNrStepAttributes( id_step, TOPICS );
    for ( int i = 0; i < topicCount; i++ ) {
      topics.add( rep.getStepAttributeString( id_step, i, TOPICS ) );
    }
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId transId, ObjectId stepId )
    throws KettleException {
    super.saveRep( rep, metaStore, transId, stepId );
    rep.saveStepAttribute( transId, stepId, MQTT_SERVER, mqttServer  );
    rep.saveStepAttribute( transId, stepId, MSG_OUTPUT_NAME, msgOutputName  );
    rep.saveStepAttribute( transId, stepId, TOPIC_OUTPUT_NAME, topicOutputName  );
    int i = 0;
    for ( String topic : topics ) {
      rep.saveStepAttribute( transId, stepId, i++, TOPICS, topic );
    }
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
}
