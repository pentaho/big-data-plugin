package com.pentaho.di.plugins.catalog.common;

import org.pentaho.di.core.exception.KettleException;

public interface WriterInterface {
  public boolean processRow( MetaAdaptorInterface smi, DataAdaptorInterface sdi ) throws KettleException;
}
