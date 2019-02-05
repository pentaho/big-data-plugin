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

import org.pentaho.metaverse.api.IAnalysisContext;
import org.pentaho.metaverse.api.analyzer.kettle.step.BaseStepExternalResourceConsumer;
import org.pentaho.metaverse.api.model.IExternalResourceInfo;

import java.util.Collection;
import java.util.HashSet;

public class KafkaConsumerERC extends BaseStepExternalResourceConsumer {
  @Override public Collection<IExternalResourceInfo> getResourcesFromMeta( Object consumer, IAnalysisContext context ) {
    KafkaConsumerInputMeta meta = (KafkaConsumerInputMeta) consumer;
    Collection<IExternalResourceInfo> resources = new HashSet<>();

    for ( String topic : meta.getTopics() ) {
      KafkaResourceInfo kafkaResourceInfo = new KafkaResourceInfo( meta.getBootstrapServers(), topic );
      resources.add( kafkaResourceInfo );
    }

    return resources;
  }

  @Override public Class getMetaClass() {
    return KafkaProducerOutputMeta.class;
  }
}
