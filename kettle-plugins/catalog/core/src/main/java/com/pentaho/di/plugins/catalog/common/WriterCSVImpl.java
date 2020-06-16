package com.pentaho.di.plugins.catalog.common;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.steps.textfileoutput.TextFileOutput;
import org.pentaho.di.trans.steps.textfileoutput.TextFileOutputData;
import org.pentaho.di.trans.steps.textfileoutput.TextFileOutputMeta;

public class WriterCSVImpl implements WriterInterface {

  private TextFileOutput tfo;
  private TextFileOutputMeta tfom;
  private TextFileOutputData tfod;
  private boolean first = true;

  @Override public boolean processRow( MetaAdaptorInterface mai, DataAdaptorInterface dai ) throws KettleException {
    if ( first ) {
      first = false;
      tfom = new TextFileOutputMeta();
      tfod = new TextFileOutputData();
      tfo = new TextFileOutput( mai.getStepMeta(),
        mai.getStepDataInterface(),
        0,
        mai.getTransMeta(),
        mai.getTrans() );
      tfo.setInputRowMeta( dai.getInputRowMeta() );
      // Determine how we are going to writeout the file.
      tfom.setSeparator( "," );
      tfom.setEndedLine( "\n" );
      tfom.setFileFormat( "csv" );
      tfom.setFileAppended( false );
      tfom.setFilename( dai.getFullFileName() );

      // setup data
      tfod.outputRowMeta = dai.getOutputRowMeta();
    }
    return tfo.processRow( tfom, tfod ) ;
  }
}
