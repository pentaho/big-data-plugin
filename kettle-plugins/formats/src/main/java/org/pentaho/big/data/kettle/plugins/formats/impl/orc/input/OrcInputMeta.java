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

package org.pentaho.big.data.kettle.plugins.formats.impl.orc.input;

import org.pentaho.big.data.kettle.plugins.formats.impl.NamedClusterResolver;
import org.pentaho.big.data.kettle.plugins.formats.orc.input.OrcInputMetaBase;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;

//keep ID as new because we will have old step with ID OrcInput
@Step( id = "OrcInput", image = "OI.svg", name = "OrcInput.Name", description = "OrcInput.Description",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
    documentationUrl = "mk-95pdia003/pdi-transformation-steps/orc-input",
    i18nPackageName = "org.pentaho.di.trans.steps.orc" )
@InjectionSupported( localizationPrefix = "OrcInput.Injection.", groups = { "FILENAME_LINES", "FIELDS" }, hide = {
  "FILEMASK", "EXCLUDE_FILEMASK", "FILE_REQUIRED", "INCLUDE_SUBFOLDERS", "FIELD_POSITION", "FIELD_LENGTH",
  "FIELD_IGNORE", "FIELD_FORMAT", "FIELD_PRECISION", "FIELD_CURRENCY",
  "FIELD_DECIMAL", "FIELD_GROUP", "FIELD_REPEAT", "FIELD_TRIM_TYPE", "FIELD_NULL_STRING", "FIELD_IF_NULL",
  "FIELD_NULLABLE", "ACCEPT_FILE_NAMES", "ACCEPT_FILE_STEP", "PASS_THROUGH_FIELDS", "ACCEPT_FILE_FIELD",
  "ADD_FILES_TO_RESULT", "IGNORE_ERRORS", "FILE_ERROR_FIELD", "FILE_ERROR_MESSAGE_FIELD", "SKIP_BAD_FILES",
  "WARNING_FILES_TARGET_DIR", "WARNING_FILES_EXTENTION",
  "ERROR_FILES_TARGET_DIR", "ERROR_FILES_EXTENTION", "LINE_NR_FILES_TARGET_DIR", "LINE_NR_FILES_EXTENTION",
  "FILE_SHORT_FILE_FIELDNAME",
  "FILE_EXTENSION_FIELDNAME", "FILE_PATH_FIELDNAME", "FILE_SIZE_FIELDNAME", "FILE_HIDDEN_FIELDNAME",
  "FILE_LAST_MODIFICATION_FIELDNAME",
  "FILE_URI_FIELDNAME", "FILE_ROOT_URI_FIELDNAME", "FIELD_SOURCE_TYPE"
} )
public class OrcInputMeta extends OrcInputMetaBase {

  private final NamedClusterResolver namedClusterResolver;

  public OrcInputMeta( NamedClusterResolver namedClusterResolver ) {
    this.namedClusterResolver = namedClusterResolver;
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    return new OrcInput( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new OrcInputData();
  }

  public NamedClusterResolver getNamedClusterResolver() {
    return namedClusterResolver;
  }
}
