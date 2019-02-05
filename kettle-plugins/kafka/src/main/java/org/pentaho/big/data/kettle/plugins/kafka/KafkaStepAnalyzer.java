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

import org.pentaho.dictionary.DictionaryHelper;

import static org.pentaho.dictionary.DictionaryConst.CATEGORY_DATASOURCE;
import static org.pentaho.dictionary.DictionaryConst.CATEGORY_MESSAGE_QUEUE;
import static org.pentaho.dictionary.DictionaryConst.LINK_CONTAINS_CONCEPT;
import static org.pentaho.dictionary.DictionaryConst.LINK_PARENT_CONCEPT;
import static org.pentaho.dictionary.DictionaryConst.NODE_TYPE_EXTERNAL_CONNECTION;

class KafkaStepAnalyzer {
  private KafkaStepAnalyzer() { }

  static final String NODE_TYPE_KAFKA_TOPIC = "Kafka Topic";
  static final String NODE_TYPE_KAFKA_SERVER = "Kafka Server";
  static final String KEY = "Key";
  static final String MESSAGE = "Message";

  static {
    DictionaryHelper.typeCategoryMap.put( NODE_TYPE_KAFKA_SERVER, CATEGORY_DATASOURCE );
    DictionaryHelper.typeCategoryMap.put( NODE_TYPE_KAFKA_TOPIC, CATEGORY_MESSAGE_QUEUE );
    DictionaryHelper.registerEntityType( LINK_PARENT_CONCEPT, NODE_TYPE_KAFKA_SERVER, NODE_TYPE_EXTERNAL_CONNECTION );
    DictionaryHelper.registerEntityType( LINK_PARENT_CONCEPT, NODE_TYPE_KAFKA_TOPIC, null );
    DictionaryHelper.registerEntityType( LINK_CONTAINS_CONCEPT, NODE_TYPE_KAFKA_TOPIC, NODE_TYPE_KAFKA_SERVER );
  }

  public static void registerEntityTypes() {
    // this method is just here to poke the class and make sure it gets loaded so the static block executes.
  }

}

