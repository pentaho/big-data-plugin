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

import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.util.Collections;

public class MQTTProducer extends BaseStep implements StepInterface {
  private static Class<?> PKG = MQTTProducerMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  private static final String PROTOCOL = "tcp://";

  private MQTTProducerMeta meta;
  private MQTTProducerData data;

  /**
   * This is the base step that forms that basis for all steps. You can derive from this class to implement your own
   * steps.
   *
   * @param stepMeta          The StepMeta object to run.
   * @param stepDataInterface the data object to store temporary data, database connections, caches, result sets,
   *                          hashtables etc.
   * @param copyNr            The copynumber for this step.
   * @param transMeta         The TransInfo of which the step stepMeta is part of.
   * @param trans             The (running) transformation to obtain information shared among the steps.
   */
  public MQTTProducer( StepMeta stepMeta,
                       StepDataInterface stepDataInterface, int copyNr,
                       TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    boolean isInitalized = super.init( stepMetaInterface, stepDataInterface );
    meta = ( (MQTTProducerMeta) stepMetaInterface );
    data = ( (MQTTProducerData) stepDataInterface );

    return isInitalized;
  }

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    Object[] row = getRow();

    if ( null == row ) {
      setOutputDone();
      if ( null != data.mqttClient ) {
        try {
          data.mqttClient.disconnect();
          data.mqttClient.close();
        } catch ( MqttException e ) {
          logError( e.getMessage(), e );
        }
      }

      return false;
    }

    if ( first ) {
      logDebug( "Publishing using a quality of service level of " + environmentSubstitute( meta.getQOS() ) );
      data.messageFieldIndex = getInputRowMeta().indexOfValue( environmentSubstitute( meta.getMessageField() ) );
      try {
        data.mqttClient = MQTTClientBuilder.builder()
          .withBroker( meta.getMqttServer() )
          .withTopics( Collections.singletonList( meta.getTopic() ) )
          .withQos( meta.getQOS() )
          .withStep( this )
          .withUsername( meta.getUsername() )
          .withPassword( meta.getPassword() )
          .buildAndConnect();
      } catch ( MqttException e ) {
        stopAll();
        logError( e.getMessage() );
        return false;
      }

      first = false;
    }

    MqttMessage mqttMessage = new MqttMessage();
    try {
      mqttMessage.setQos( Integer.parseInt( environmentSubstitute( meta.getQOS() ) ) );
    } catch ( NumberFormatException e ) {
      throw new KettleStepException(
        BaseMessages.getString( PKG, "MQTTProducer.Error.QOS", environmentSubstitute( meta.getQOS() ) ) );
    }
    mqttMessage.setPayload( ( row[ data.messageFieldIndex ] ).toString().getBytes() );

    try {
      data.mqttClient.publish( environmentSubstitute( meta.getTopic() ), mqttMessage );

      incrementLinesOutput();
      putRow( getInputRowMeta(), row ); // copy row to possible alternate rowset(s).

      if ( checkFeedback( getLinesRead() ) ) {
        if ( log.isBasic() ) {
          logBasic( BaseMessages.getString( PKG, "MQTTProducer.Log.LineNumber" ) + getLinesRead() );
        }
      }
    } catch ( MqttException e ) {
      logError( e.getMessage(), e );
    }

    return true;
  }
}
