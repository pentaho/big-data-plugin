package org.pentaho.big.data.kettle.plugins.formats.parquet.output;

import org.pentaho.big.data.kettle.plugins.formats.parquet.output.ParquetOutputMetaBase;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;

@Step( id = "ParquetOutput", image = "HBO.svg", name = "ParquetOutput.Name", description = "ParquetOutput.Description",
categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
documentationUrl = "http://wiki.pentaho.com/display/EAI/Parquet+output",
i18nPackageName = "org.pentaho.di.trans.steps.parquet", isSeparateClassLoaderNeeded = true )
public class ParquetOutputMeta extends ParquetOutputMetaBase {

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    return new ParquetOutput( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new ParquetOutputData();
  }

}
