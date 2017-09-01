/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.pentaho.bigdata.api.jaas.JaasConfigService;

/**
 * Created by rfellows on 6/2/17.
 */
public class KafkaFactory {
  private Function<Map<String, Object>, Consumer> consumerFunction;
  private Function<Map<String, Object>, Producer<Object, Object>> producerFunction;

  public static KafkaFactory defaultFactory() {
    return new KafkaFactory( KafkaConsumer::new, KafkaProducer::new );
  }

  KafkaFactory(
    Function<Map<String, Object>, Consumer> consumerFunction,
    Function<Map<String, Object>, Producer<Object, Object>> producerFunction ) {
    this.consumerFunction = consumerFunction;
    this.producerFunction = producerFunction;
  }

  public Consumer consumer( KafkaConsumerInputMeta meta, Function<String, String> variablesFunction ) {
    return consumer( meta, variablesFunction, KafkaConsumerField.Type.String, KafkaConsumerField.Type.String );
  }

  public Consumer consumer( KafkaConsumerInputMeta meta, Function<String, String> variablesFunction,
    KafkaConsumerField.Type keyDeserializerType, KafkaConsumerField.Type msgDeserializerType ) {

    HashMap<String, Object> kafkaConfig = new HashMap<>();
    Function<String, String> variableNonNull = variablesFunction.andThen( KafkaFactory::nullToEmpty );
    kafkaConfig.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, variableNonNull.apply( meta.getBootstrapServers() ) );
    kafkaConfig.put( ConsumerConfig.GROUP_ID_CONFIG, variableNonNull.apply( meta.getConsumerGroup() ) );
    kafkaConfig.put( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, msgDeserializerType.getKafkaDeserializerClass() );
    kafkaConfig.put( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerType.getKafkaDeserializerClass() );
    meta.getJaasConfigService().ifPresent( jaasConfigService -> putKerberosConfig( kafkaConfig, jaasConfigService ) );
    meta.getConfig().entrySet()
        .forEach( ( entry -> kafkaConfig.put( entry.getKey(), variableNonNull.apply(
            (String) entry.getValue() ) ) ) );

    return consumerFunction.apply( kafkaConfig );
  }

  public void putKerberosConfig( HashMap<String, Object> kafkaConfig, JaasConfigService jaasConfigService ) {
    if ( jaasConfigService.isKerberos() ) {
      kafkaConfig.put( SaslConfigs.SASL_JAAS_CONFIG, jaasConfigService.getJaasConfig() );
      kafkaConfig.put( CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT" );
    }
  }

  public Producer<Object, Object> producer(
    KafkaProducerOutputMeta meta, Function<String, String> variablesFunction ) {
    return producer( meta, variablesFunction, KafkaConsumerField.Type.String, KafkaConsumerField.Type.String );
  }

  public Producer<Object, Object> producer(
    KafkaProducerOutputMeta meta, Function<String, String> variablesFunction,
    KafkaConsumerField.Type keySerializerType, KafkaConsumerField.Type msgSerializerType ) {

    Function<String, String> variableNonNull = variablesFunction.andThen( KafkaFactory::nullToEmpty );
    HashMap<String, Object> kafkaConfig = new HashMap<>();
    kafkaConfig.put( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, variableNonNull.apply( meta.getBootstrapServers() ) );
    kafkaConfig.put( ProducerConfig.CLIENT_ID_CONFIG, variableNonNull.apply( meta.getClientId() ) );
    kafkaConfig.put( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, msgSerializerType.getKafkaSerializerClass() );
    kafkaConfig.put( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerType.getKafkaSerializerClass() );
    meta.getJaasConfigService().ifPresent( jaasConfigService -> putKerberosConfig( kafkaConfig, jaasConfigService ) );
    meta.getConfig().entrySet()
        .forEach( ( entry -> kafkaConfig.put( entry.getKey(), variableNonNull.apply(
            (String) entry.getValue() ) ) ) );

    return producerFunction.apply( kafkaConfig );
  }

  private static String nullToEmpty( String value ) {
    return value == null ? "" : value;
  }
}
