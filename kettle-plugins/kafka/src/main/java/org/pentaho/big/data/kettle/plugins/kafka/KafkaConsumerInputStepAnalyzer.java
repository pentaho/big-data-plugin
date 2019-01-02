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

import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.metaverse.api.IMetaverseNode;
import org.pentaho.metaverse.api.MetaverseAnalyzerException;
import org.pentaho.metaverse.api.StepField;
import org.pentaho.metaverse.api.analyzer.kettle.step.StepAnalyzer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class KafkaConsumerInputStepAnalyzer extends StepAnalyzer<KafkaConsumerInputMeta> {
  @Override
  protected Set<StepField> getUsedFields( KafkaConsumerInputMeta meta ) {
    // no incoming fields are used by the Dummy step
    return Collections.emptySet();
  }

  @Override
  protected void customAnalyze( KafkaConsumerInputMeta meta, IMetaverseNode rootNode )
    throws MetaverseAnalyzerException {
    // add any custom properties or relationships here
    rootNode.setProperty( "do_nothing", true );
  }

  @Override
  public Set<Class<? extends BaseStepMeta>> getSupportedSteps() {
    Set<Class<? extends BaseStepMeta>> supportedSteps = new HashSet<>();
    supportedSteps.add( KafkaConsumerInputMeta.class );
    return supportedSteps;
  }
}
