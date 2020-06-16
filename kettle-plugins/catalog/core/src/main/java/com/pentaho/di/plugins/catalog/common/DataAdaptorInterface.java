package com.pentaho.di.plugins.catalog.common;

import org.pentaho.di.core.row.RowMetaInterface;

public interface DataAdaptorInterface {
  public RowMetaInterface getOutputRowMeta();
  public RowMetaInterface getInputRowMeta();
  public String getFullFileName();
}
