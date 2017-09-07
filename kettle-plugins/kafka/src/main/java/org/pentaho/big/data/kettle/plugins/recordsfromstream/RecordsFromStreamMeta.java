/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.recordsfromstream;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.trans.steps.rowsfromresult.RowsFromResultMeta;

@Step( id = "RecordsFromStream", image = "get-records-from-stream.svg",
  i18nPackageName = "org.pentaho.big.data.kettle.plugins.recordsfromstream",
  name = "RecordsFromStream.TypeLongDesc",
  description = "RecordsFromStream.TypeTooltipDesc",
  categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Streaming" )
public class RecordsFromStreamMeta extends RowsFromResultMeta {
}
