/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.hdfs.trans.analyzer;

import org.pentaho.big.data.kettle.plugins.hdfs.trans.HadoopFileOutputMeta;
import org.pentaho.metaverse.api.IMetaverseNode;
import org.pentaho.metaverse.api.MetaverseAnalyzerException;
import org.pentaho.metaverse.api.analyzer.kettle.step.IClonableStepAnalyzer;

public class HadoopFileOutputStepAnalyzer extends HadoopBaseStepAnalyzer<HadoopFileOutputMeta> {

  @Override
  public Class<HadoopFileOutputMeta> getMetaClass() {
    return HadoopFileOutputMeta.class;
  }

  @Override public boolean isOutput() {
    return true;
  }

  @Override public boolean isInput() {
    return false;
  }

  @Override
  protected void customAnalyze( final HadoopFileOutputMeta meta, final IMetaverseNode rootNode )
    throws MetaverseAnalyzerException {
    super.customAnalyze( meta, rootNode );
    rootNode.setProperty( "createParentFolder", meta.isCreateParentFolder() );
    rootNode.setProperty( "doNotOpenNewFileInit", meta.isDoNotOpenNewFileInit() );
    if ( meta.isFileNameInField() ) {
      rootNode.setProperty( "fileNameField", meta.getFileNameField() );
    }
    rootNode.setProperty( "extension", meta.getExtension() );
    rootNode.setProperty( "stepNrInFilename", meta.isStepNrInFilename() );
    rootNode.setProperty( "partNrInFilename", meta.isPartNrInFilename() );
    rootNode.setProperty( "dateInFilename", meta.isDateInFilename() );
    rootNode.setProperty( "timeInFilename", meta.isTimeInFilename() );
    if ( meta.isSpecifyingFormat() ) {
      rootNode.setProperty( "dateTimeFormat", meta.getDateTimeFormat() );
    }
    rootNode.setProperty( "addFilenamesToResult", meta.isAddToResultFiles() );
    rootNode.setProperty( "append", meta.isFileAppended() );
    rootNode.setProperty( "separator", meta.getSeparator() );
    rootNode.setProperty( "enclosure", meta.getEnclosure() );
    rootNode.setProperty( "forceEnclosure", meta.isEnclosureForced() );
    rootNode.setProperty( "addHeader", meta.isHeaderEnabled() );
    rootNode.setProperty( "addFooter", meta.isFooterEnabled() );
    rootNode.setProperty( "fileFormat", meta.getFileFormat() );
    rootNode.setProperty( "fileCompression", meta.getFileCompression() );
    rootNode.setProperty( "encoding", meta.getEncoding() );
    rootNode.setProperty( "rightPadFields", meta.isPadded() );
    rootNode.setProperty( "fastDataDump", meta.isFastDump() );
    rootNode.setProperty( "splitEveryRows", meta.getSplitEveryRows() );
    rootNode.setProperty( "endingLine", meta.getEndedLine() );
  }

  @Override
  public IClonableStepAnalyzer newInstance() {
    return new HadoopFileOutputStepAnalyzer();
  }
}
