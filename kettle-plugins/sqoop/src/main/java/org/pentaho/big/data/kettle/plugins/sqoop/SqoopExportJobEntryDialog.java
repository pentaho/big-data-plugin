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
import org.pentaho.big.data.kettle.plugins.sqoop.ui.SqoopExportJobEntryController;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.BindingFactory;

import java.lang.reflect.InvocationTargetException;

/**
 * Dialog for the Sqoop Export job entry.
 * 
 * @see SqoopExportJobEntry
 */
public class SqoopExportJobEntryDialog extends AbstractSqoopJobEntryDialog<SqoopExportConfig, SqoopExportJobEntry> {

  public SqoopExportJobEntryDialog( Shell parent, JobEntryInterface jobEntry, Repository rep, JobMeta jobMeta )
    throws XulException, InvocationTargetException {
    super( parent, jobEntry, rep, jobMeta );
  }

  @Override
  protected String getXulFile() {
    return "org/pentaho/big/data/kettle/plugins/sqoop/xul/SqoopExportJobEntry.xul";
  }

  @Override
  protected Class<?> getMessagesClass() {
    return SqoopExportJobEntry.class;
  }

  @Override
  protected SqoopExportJobEntryController createController( XulDomContainer container, SqoopExportJobEntry jobEntry,
                                                            BindingFactory bindingFactory ) {
    return new SqoopExportJobEntryController( jobMeta, container, jobEntry, bindingFactory );
  }
}
