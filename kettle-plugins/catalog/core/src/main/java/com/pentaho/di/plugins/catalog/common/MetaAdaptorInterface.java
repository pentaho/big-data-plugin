package com.pentaho.di.plugins.catalog.common;

import org.pentaho.big.data.kettle.plugins.formats.impl.NamedClusterResolver;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;

public interface MetaAdaptorInterface {
  TransMeta getTransMeta();
  StepMeta getStepMeta();
  StepDataInterface getStepDataInterface();
  Trans getTrans();
  NamedClusterResolver getNamedClusterResolver();
}
