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


package org.pentaho.big.data.kettle.plugins.sqoop;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.big.data.kettle.plugins.sqoop.ui.AbstractSqoopJobEntryDialog;
import org.pentaho.big.data.kettle.plugins.sqoop.ui.SqoopImportJobEntryController;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.BindingFactory;

/**
 * Dialog for the Sqoop Import Job Entry
 * 
 * @see SqoopImportJobEntry
 */
public class SqoopImportJobEntryDialog extends AbstractSqoopJobEntryDialog<SqoopImportConfig, SqoopImportJobEntry> {

  public SqoopImportJobEntryDialog( Shell parent, JobEntryInterface jobEntry, Repository rep, JobMeta jobMeta )
    throws XulException {
    super( parent, jobEntry, rep, jobMeta );
  }

  @Override
  protected Class<?> getMessagesClass() {
    return SqoopImportJobEntry.class;
  }

  @Override
  protected SqoopImportJobEntryController createController( XulDomContainer container, SqoopImportJobEntry jobEntry,
                                                            BindingFactory bindingFactory ) {
    return new SqoopImportJobEntryController( jobMeta, container, jobEntry, bindingFactory );
  }

  @Override
  protected String getXulFile() {
    return "org/pentaho/big/data/kettle/plugins/sqoop/xul/SqoopImportJobEntry.xul";
  }
}
