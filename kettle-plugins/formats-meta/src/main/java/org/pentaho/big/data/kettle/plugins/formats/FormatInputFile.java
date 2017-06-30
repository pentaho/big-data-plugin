package org.pentaho.big.data.kettle.plugins.formats;

import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.trans.steps.file.BaseFileInputFiles;

public class FormatInputFile extends BaseFileInputFiles {
  @Injection( name = "ENVIRONMENT", group = "FILENAME_LINES" )
  private String environment;

  public String getEnvironment() {
    return environment;
  }

  public void setEnvironment( String environment ) {
    this.environment = environment;
  }
}
