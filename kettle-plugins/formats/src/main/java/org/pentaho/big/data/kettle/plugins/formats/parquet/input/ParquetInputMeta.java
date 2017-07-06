package org.pentaho.big.data.kettle.plugins.formats.parquet.input;

import org.pentaho.big.data.kettle.plugins.formats.parquet.input.ParquetInputMetaBase;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;

@Step( id = "ParquetInput", image = "HBO.svg", name = "ParquetInput.Name", description = "ParquetInput.Description",
categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
documentationUrl = "http://wiki.pentaho.com/display/EAI/HBase+Input",
i18nPackageName = "org.pentaho.di.trans.steps.parquet", isSeparateClassLoaderNeeded = true )
public class ParquetInputMeta extends ParquetInputMetaBase {

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    return new ParquetInput( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new ParquetInputData();
  }
}
