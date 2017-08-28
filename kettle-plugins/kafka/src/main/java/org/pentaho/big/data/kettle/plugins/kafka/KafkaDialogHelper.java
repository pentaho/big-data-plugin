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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SslConfigs;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;

public class KafkaDialogHelper {
  private ComboVar wTopic;
  private ComboVar wClusterName;
  private KafkaFactory kafkaFactory;
  private NamedClusterService namedClusterService;
  private MetastoreLocator metastoreLocator;
  private NamedClusterServiceLocator namedClusterServiceLocator;

  public KafkaDialogHelper( ComboVar wClusterName, ComboVar wTopic, KafkaFactory kafkaFactory,
                            NamedClusterService namedClusterService,
                            NamedClusterServiceLocator namedClusterServiceLocator, MetastoreLocator metastoreLocator ) {
    this.wClusterName = wClusterName;
    this.wTopic = wTopic;
    this.kafkaFactory = kafkaFactory;
    this.namedClusterService = namedClusterService;
    this.metastoreLocator = metastoreLocator;
    this.namedClusterServiceLocator = namedClusterServiceLocator;
  }

  public void clusterNameChanged( @SuppressWarnings( "unused" ) Event event ) {
    String current = wTopic.getText();
    if ( StringUtil.isEmpty( wClusterName.getText() ) ) {
      return;
    }
    String clusterName = wClusterName.getText();
    CompletableFuture
      .supplyAsync( () -> listTopics( clusterName ) )
      .thenAccept( ( topicMap ) -> Display.getDefault().syncExec( () -> populateTopics( topicMap, current ) ) );
  }

  private void populateTopics( Map<String, List<PartitionInfo>> topicMap, String current ) {
    if ( !wTopic.getCComboWidget().isDisposed() ) {
      wTopic.getCComboWidget().removeAll();
    }
    topicMap.keySet().stream()
      .filter( key -> !"__consumer_offsets".equals( key ) ).sorted().forEach( key -> {
        if ( !wTopic.isDisposed() ) {
          wTopic.add( key );
        }
      } );
    if ( !wTopic.getCComboWidget().isDisposed() ) {
      wTopic.getCComboWidget().setText( current );
    }
  }

  private Map<String, List<PartitionInfo>> listTopics( String clusterName ) {
    Consumer kafkaConsumer = null;
    try {
      KafkaConsumerInputMeta localMeta = new KafkaConsumerInputMeta();
      localMeta.setNamedClusterService( namedClusterService );
      localMeta.setNamedClusterServiceLocator( namedClusterServiceLocator );
      localMeta.setMetastoreLocator( metastoreLocator );
      localMeta.setClusterName( clusterName );
      kafkaConsumer = kafkaFactory.consumer( localMeta, Function.identity() );
      @SuppressWarnings( "unchecked" ) Map<String, List<PartitionInfo>> topicMap = kafkaConsumer.listTopics();
      return topicMap;
    } finally {
      if ( kafkaConsumer != null ) {
        kafkaConsumer.close();
      }
    }
  }

  public static List<String> getConsumerConfigOptionNames() {
    List<String> optionNames = getConfigOptionNames( ConsumerConfig.class );
    Stream.of( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumerConfig.GROUP_ID_CONFIG,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ).forEach( optionNames::remove );
    return optionNames;
  }

  public static List<String> getProducerConfigOptionNames() {
    List<String> optionNames = getConfigOptionNames( ProducerConfig.class );
    Stream.of( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerConfig.CLIENT_ID_CONFIG,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG )
        .forEach( optionNames::remove );
    return optionNames;
  }

  public static List<String> getConsumerAdvancedConfigOptionNames() {
    return Arrays.asList( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, SslConfigs.SSL_KEY_PASSWORD_CONFIG,
        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG );
  }

  public static List<String> getProducerAdvancedConfigOptionNames() {
    return Arrays.asList( ProducerConfig.COMPRESSION_TYPE_CONFIG, SslConfigs.SSL_KEY_PASSWORD_CONFIG,
        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG );
  }

  private static List<String> getConfigOptionNames( Class cl ) {
    return getStaticField( cl, "CONFIG" ).map( config ->
        ( (ConfigDef) config ).configKeys().keySet().stream().sorted().collect( Collectors.toList() )
    ).orElse( new ArrayList<>() );
  }

  private static Optional<Object> getStaticField( Class cl, String fieldName ) {
    Field field = null;
    boolean isAccessible = false;
    try {
      field = cl.getDeclaredField( fieldName );
      isAccessible = field.isAccessible();
      field.setAccessible( true );
      return Optional.ofNullable( field.get( null ) );
    } catch ( NoSuchFieldException | IllegalAccessException e ) {
      return Optional.empty();
    } finally {
      if ( field != null ) {
        field.setAccessible( isAccessible );
      }
    }
  }
}
