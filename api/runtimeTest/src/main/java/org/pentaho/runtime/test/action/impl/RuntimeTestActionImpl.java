/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.runtime.test.action.impl;

import org.pentaho.runtime.test.action.RuntimeTestAction;
import org.pentaho.runtime.test.action.RuntimeTestActionPayload;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;

/**
 * Created by bryan on 9/9/15.
 */
public class RuntimeTestActionImpl implements RuntimeTestAction {
  private final String name;
  private final String description;
  private final RuntimeTestEntrySeverity severity;
  private final RuntimeTestActionPayload payload;

  public RuntimeTestActionImpl( String name, String description, RuntimeTestEntrySeverity severity,
                                RuntimeTestActionPayload payload ) {
    this.name = name;
    this.description = description;
    this.severity = severity;
    this.payload = payload;
  }

  @Override public String getName() {
    return name;
  }

  @Override public String getDescription() {
    return description;
  }

  @Override public RuntimeTestEntrySeverity getSeverity() {
    return severity;
  }

  @Override public RuntimeTestActionPayload getPayload() {
    return payload;
  }
}
