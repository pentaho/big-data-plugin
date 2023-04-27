/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.pentaho.di.job.entries.spark;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.pentaho.di.job.entry.loadSave.JobEntryLoadSaveTestSupport;
import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.MapLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.StringLoadSaveValidator;

import static java.util.Arrays.asList;

public class JobEntrySparkSubmitLoadSaveTest extends JobEntryLoadSaveTestSupport<JobEntrySparkSubmit> {
  @Override
  protected Class<JobEntrySparkSubmit> getJobEntryClass() {
    return JobEntrySparkSubmit.class;
  }

  @Override
  protected Map<String, FieldLoadSaveValidator<?>> createTypeValidatorsMap() {
    Map<String, FieldLoadSaveValidator<?>> validators = new HashMap<>();

    validators.put( "java.util.Map<java.lang.String,java.lang.String>", new MapLoadSaveValidator<>(
        new StringLoadSaveValidator(), new StringLoadSaveValidator() ) );
    return validators;
  }
  @Override
  protected List<String> listCommonAttributes() {
    return asList( "scriptPath", "master", "jar", "className", "args", "configParams", "driverMemory",
        "executorMemory", "blockExecution", "jobType", "pyFile", "libs" );
  }
}
