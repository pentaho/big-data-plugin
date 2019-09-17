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

import org.pentaho.metaverse.api.model.IExternalResourceInfo;

import java.util.HashMap;
import java.util.Map;

public class KafkaResourceInfo implements IExternalResourceInfo {
  private final String bootstrapServer;

  private final String topic;


  KafkaResourceInfo( String bootstrapServer, String topic ) {
    this.bootstrapServer = bootstrapServer;
    this.topic = topic;
  }

  @Override public String getType() {
    return KafkaStepAnalyzer.NODE_TYPE_KAFKA_TOPIC;
  }

  @Override public boolean isInput() {
    return false;
  }

  @Override public boolean isOutput() {
    return true;
  }

  @Override public Map<Object, Object> getAttributes() {
    HashMap<Object, Object> attributes = new HashMap<>();
    attributes.put( "Topic", topic );
    return attributes;
  }

  @Override public String getName() {
    return bootstrapServer;
  }

  @Override public void setName( String name ) {
    throw new UnsupportedOperationException( "name should have be passed in constructor" );
  }

  @Override public String getDescription() {
    return "Kafka Event Queue";
  }

  @Override public void setDescription( String description ) {
    throw new UnsupportedOperationException( "description is constant and should not change" );
  }

  public String getTopic() {
    return topic;
  }
}
