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
package org.pentaho.big.data.impl.cluster.tests.kafka;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.big.data.impl.cluster.tests.ClusterRuntimeTestEntry;
import org.pentaho.big.data.impl.cluster.tests.Constants;
import org.pentaho.hadoop.shim.api.jaas.JaasConfigService;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;
import org.pentaho.runtime.test.result.org.pentaho.runtime.test.result.impl.RuntimeTestResultSummaryImpl;
import org.pentaho.runtime.test.test.impl.BaseRuntimeTest;
import org.pentaho.runtime.test.test.impl.RuntimeTestResultEntryImpl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class KafkaConnectTest extends BaseRuntimeTest {
  public static final String KAFKA_CONNECT_TEST = "KafkaConnectTest";
  public static final String KAFKA_CONNECT_TEST_NAME = "KafkaConnectTest.Name";
  public static final String KAFKA_CONNECT_TEST_MALFORMED_URL_DESC = "KafkaConnectTest.MalformedUrl.Desc";
  public static final String KAFKA_CONNECT_TEST_MALFORMED_URL_MESSAGE = "KafkaConnectTest.MalformedUrl.Message";
  public static final String KAFKA_CONNECT_TEST_SUCCESS_DESC = "KafkaConnectTest.Success.Desc";
  public static final String KAFKA_CONNECT_TEST_SUCCESS_MESSAGE = "KafkaConnectTest.Success.Message";
  public static final String KAFKA_CONNECT_TEST_EMPTY_DESC = "KafkaConnectTest.Empty.Desc";
  public static final String KAFKA_CONNECT_TEST_EMPTY_MESSAGE = "KafkaConnectTest.Empty.Message";
  private final MessageGetter messageGetter;
  Function<Map<String, Object>, Consumer> consumerFunction;
  static final Class<?> PKG = KafkaConnectTest.class;
  protected final MessageGetterFactory messageGetterFactory;
  private NamedClusterServiceLocator namedClusterServiceLocator;

  public KafkaConnectTest( MessageGetterFactory messageGetterFactory, NamedClusterServiceLocator namedClusterServiceLocator ) {
    this( messageGetterFactory, KafkaConsumer::new, namedClusterServiceLocator );
  }

  KafkaConnectTest( MessageGetterFactory messageGetterFactory, Function<Map<String, Object>, Consumer> consumerFunction,
                    final NamedClusterServiceLocator namedClusterServiceLocator ) {
    super( NamedCluster.class, Constants.KAFKA, KAFKA_CONNECT_TEST,
      messageGetterFactory.create( PKG ).getMessage( KAFKA_CONNECT_TEST_NAME ), Collections.emptySet() );
    this.messageGetterFactory = messageGetterFactory;
    this.namedClusterServiceLocator = namedClusterServiceLocator;
    messageGetter = messageGetterFactory.create( PKG );
    this.consumerFunction = consumerFunction;
  }

  @Override public RuntimeTestResultSummary runTest( final Object objectUnderTest ) {
    NamedCluster namedCluster = (NamedCluster) objectUnderTest;
    // The connection information might be parameterized. Since we aren't tied to a transformation or job, in order to
    // use a parameter, the value would have to be set as a system property or in kettle.properties, etc.
    // Here we try to resolve the parameters if we can:
    Variables variables = new Variables();
    variables.initializeVariablesFrom( null );
    String bootstrapServers = variables.environmentSubstitute( namedCluster.getKafkaBootstrapServers() );
    if ( StringUtils.isBlank( bootstrapServers ) ) {
      return new RuntimeTestResultSummaryImpl( new ClusterRuntimeTestEntry(
        messageGetterFactory,
        new RuntimeTestResultEntryImpl(
          RuntimeTestEntrySeverity.SKIPPED,
          messageGetter.getMessage( KAFKA_CONNECT_TEST_EMPTY_DESC ),
          messageGetter.getMessage( KAFKA_CONNECT_TEST_EMPTY_MESSAGE ) ),
        ClusterRuntimeTestEntry.DocAnchor.KAFKA ) );
    }
    HashMap<String, Object> configs = new HashMap<>();
    configs.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
    configs.put( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class );
    configs.put( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class );
    configs.put( ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000 );
    configs.put( ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 9000 );
    try {
      JaasConfigService jaasConfigService = namedClusterServiceLocator.getService( namedCluster, JaasConfigService.class );
      if ( jaasConfigService != null ) {
        if ( jaasConfigService.isKerberos() ) {
          configs.put( SaslConfigs.SASL_JAAS_CONFIG, jaasConfigService.getJaasConfig() );
          configs.put( CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT" );
        }
      }
    } catch ( ClusterInitializationException e ) {
      //ok, try and connect anyway.  If kafka requires kerberos we'll still get an error
    }
    try ( Consumer consumer = consumerFunction.apply( configs ) ) {
      consumer.listTopics();
      return new RuntimeTestResultSummaryImpl( new ClusterRuntimeTestEntry(
        messageGetterFactory,
        new RuntimeTestResultEntryImpl( RuntimeTestEntrySeverity.INFO,
          messageGetter.getMessage( KAFKA_CONNECT_TEST_SUCCESS_DESC ),
          messageGetter.getMessage( KAFKA_CONNECT_TEST_SUCCESS_MESSAGE ) ),
        ClusterRuntimeTestEntry.DocAnchor.KAFKA ) );
    } catch ( Exception e ) {
      return new RuntimeTestResultSummaryImpl( new ClusterRuntimeTestEntry(
        messageGetterFactory,
        new RuntimeTestResultEntryImpl(
          RuntimeTestEntrySeverity.ERROR,
          messageGetter.getMessage( KAFKA_CONNECT_TEST_MALFORMED_URL_DESC ),
          messageGetter.getMessage( KAFKA_CONNECT_TEST_MALFORMED_URL_MESSAGE, bootstrapServers ) ),
        ClusterRuntimeTestEntry.DocAnchor.KAFKA ) );
    }
  }
}

