/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
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
