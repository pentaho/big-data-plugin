/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.hdfs.trans.analyzer;

import org.pentaho.big.data.kettle.plugins.hdfs.trans.HadoopFileInputMeta;
import org.pentaho.metaverse.api.IMetaverseNode;
import org.pentaho.metaverse.api.MetaverseAnalyzerException;
import org.pentaho.metaverse.api.analyzer.kettle.step.IClonableStepAnalyzer;

public class HadoopFileInputStepAnalyzer extends HadoopBaseStepAnalyzer<HadoopFileInputMeta> {

  @Override
  public Class<HadoopFileInputMeta> getMetaClass() {
    return HadoopFileInputMeta.class;
  }

  @Override public boolean isOutput() {
    return false;
  }

  @Override public boolean isInput() {
    return true;
  }

  @Override
  protected void customAnalyze( final HadoopFileInputMeta meta, final IMetaverseNode rootNode )
    throws MetaverseAnalyzerException {
    super.customAnalyze( meta, rootNode );
    if ( meta.isAcceptingFilenames() ) {
      rootNode.setProperty( "fileNameStep", meta.getAcceptingStepName() );
      rootNode.setProperty( "fileNameField", meta.getAcceptingField() );
      rootNode.setProperty( "passingThruFields", meta.inputFiles.passingThruFields );
    }
    rootNode.setProperty( "fileType", meta.content.fileType );
    rootNode.setProperty( "separator", meta.content.separator );
    rootNode.setProperty( "enclosure", meta.content.enclosure );
    rootNode.setProperty( "breakInEnclosureAllowed", meta.content.breakInEnclosureAllowed );
    rootNode.setProperty( "escapeCharacter", meta.content.escapeCharacter );
    if ( meta.content.header ) {
      rootNode.setProperty( "nrHeaderLines", meta.content.nrHeaderLines );
    }
    if ( meta.content.footer ) {
      rootNode.setProperty( "nrFooterLines", meta.content.nrFooterLines );
    }
    if ( meta.content.lineWrapped ) {
      rootNode.setProperty( "nrWraps", meta.content.nrWraps );
    }
    if ( meta.content.layoutPaged ) {
      rootNode.setProperty( "nrLinesPerPage", meta.content.nrLinesPerPage );
      rootNode.setProperty( "nrLinesDocHeader", meta.content.nrLinesDocHeader );
    }
    rootNode.setProperty( "fileCompression", meta.content.fileCompression );
    rootNode.setProperty( "noEmptyLines", meta.content.noEmptyLines );
    rootNode.setProperty( "includeFilename", meta.content.includeFilename );
    if ( meta.content.includeFilename ) {
      rootNode.setProperty( "filenameField", meta.content.filenameField );
    }
    rootNode.setProperty( "includeRowNumber", meta.content.includeRowNumber );
    if ( meta.content.includeFilename ) {
      rootNode.setProperty( "rowNumberField", meta.content.rowNumberField );
      rootNode.setProperty( "rowNumberByFile", meta.content.rowNumberByFile );
    }
    rootNode.setProperty( "fileFormat", meta.content.fileFormat );
    rootNode.setProperty( "encoding", meta.content.encoding );
    rootNode.setProperty( "rowLimit", Long.toString( meta.content.rowLimit ) );
    rootNode.setProperty( "dateFormatLenient", meta.content.dateFormatLenient );
    rootNode.setProperty( "dateFormatLocale", meta.content.dateFormatLocale );
    rootNode.setProperty( "addFilenamesToResult", meta.inputFiles.isaddresult );
  }

  @Override
  public IClonableStepAnalyzer newInstance() {
    return new HadoopFileInputStepAnalyzer();
  }
}
