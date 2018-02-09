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

import com.google.common.base.Preconditions;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.streaming.common.BaseStreamStep;
import org.pentaho.di.trans.streaming.common.FixedTimeStreamWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.pentaho.di.i18n.BaseMessages.getString;

/**
 * Streaming consumer of MQTT input.  {@linktourl http://mqtt.org/}
 */
public class MQTTConsumer extends BaseStreamStep implements StepInterface {

  private static Class<?> PKG = MQTTConsumer.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  private MQTTConsumerMeta mqttConsumerMeta;
  private final Logger logger = LoggerFactory.getLogger( this.getClass() );

  public MQTTConsumer( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                       Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    boolean init = super.init( stepMetaInterface, stepDataInterface );
    Preconditions.checkNotNull( stepMetaInterface );
    mqttConsumerMeta = (MQTTConsumerMeta) stepMetaInterface;

    RowMeta rowMeta = new RowMeta();
    try {
      mqttConsumerMeta.getFields(
        rowMeta, getStepname(), null, null, this, repository, metaStore );
    } catch ( KettleStepException e ) {
      logger.error( getString( PKG, "MQTTInput.Error.FailureGettingFields" ), e );
    }
    window = new FixedTimeStreamWindow<>( subtransExecutor, rowMeta, getDuration(), getBatchSize() );
    try {
      final List<String> topics = mqttConsumerMeta.getTopics();
      source = new MQTTStreamSource( environmentSubstitute( mqttConsumerMeta.getMqttServer() ),
        Arrays.asList( environmentSubstitute( topics.toArray( new String[ topics.size() ] ) ) ),
        Integer.parseInt( environmentSubstitute( mqttConsumerMeta.getQos() ) ), this );
    } catch ( NumberFormatException e ) {
      logError( BaseMessages.getString( PKG, "MQTTConsumer.Error.QOS", environmentSubstitute( mqttConsumerMeta.getQos() ) ) );
      return false;
    }
    return init;
  }


}
