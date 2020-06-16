package com.pentaho.di.plugins.catalog.common;

import org.pentaho.big.data.kettle.plugins.formats.impl.parquet.output.ParquetOutput;
import org.pentaho.big.data.kettle.plugins.formats.impl.parquet.output.ParquetOutputData;
import org.pentaho.big.data.kettle.plugins.formats.impl.parquet.output.ParquetOutputMeta;
import org.pentaho.di.core.exception.KettleException;

public class WriterParquetImpl implements WriterInterface {
  ParquetOutput po;
  ParquetOutputMeta pom;
  ParquetOutputData pod;
  private boolean first = true;

  @Override public boolean processRow( MetaAdaptorInterface mai, DataAdaptorInterface dai ) throws KettleException {
    if ( first ) {
      first = false;
      pom = new ParquetOutputMeta( mai.getNamedClusterResolver() );
      pod = new ParquetOutputData();
      po = new ParquetOutput( mai.getStepMeta(),
        mai.getStepDataInterface(),
        0,
        mai.getTransMeta(),
        mai.getTrans() );
      po.setInputRowMeta( dai.getInputRowMeta() );

      pom.setExtension( "parquet" );
      pom.setCompressionType( "snappy" );
      pom.setFilename( dai.getFullFileName() );
    }
    return po.processRow( pom, pod ) ;
  }
}
