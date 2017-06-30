package org.pentaho.big.data.kettle.plugins.formats;

import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.trans.steps.file.BaseFileField;

public class FormatInputField extends BaseFileField {
  @Injection( name = "FIELD_PATH", group = "FIELDS" )
  private String path;

  public String getPath() {
    return path;
  }

  public void setPath( String path ) {
    this.path = path;
  }
}
