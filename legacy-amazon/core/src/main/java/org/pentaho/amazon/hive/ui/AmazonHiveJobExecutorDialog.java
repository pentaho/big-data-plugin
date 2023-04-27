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


package org.pentaho.amazon.hive.ui;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.annotations.PluginDialog;
import org.pentaho.amazon.AbstractAmazonJobEntry;
import org.pentaho.amazon.AbstractAmazonJobEntryDialog;
import org.pentaho.amazon.AbstractAmazonJobExecutorController;
import org.pentaho.amazon.hive.job.AmazonHiveJobExecutor;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.BindingFactory;

import java.lang.reflect.InvocationTargetException;

/**
 * Created by Aliaksandr_Zhuk on 1/24/2018.
 */
@PluginDialog( id = "HiveJobExecutorPlugin", image = "AWS-HIVE.svg", pluginType = PluginDialog.PluginType.JOBENTRY,
  documentationUrl = "Products/Amazon_Hive_Job_Executor" )
public class AmazonHiveJobExecutorDialog extends AbstractAmazonJobEntryDialog {


  public AmazonHiveJobExecutorDialog( Shell parent, JobEntryInterface jobEntry, Repository rep, JobMeta jobMeta )
    throws XulException, InvocationTargetException {
    super( parent, jobEntry, rep, jobMeta );
  }

  @Override
  protected String getXulFile() {
    return "org/pentaho/amazon/hive/ui/AmazonHiveJobExecutorDialog.xul";
  }

  @Override
  protected Class<?> getMessagesClass() {
    return AmazonHiveJobExecutor.class;
  }

  @Override
  protected AbstractAmazonJobExecutorController createController( XulDomContainer container, AbstractAmazonJobEntry jobEntry,
                                                                  BindingFactory bindingFactory ) {
    return new AmazonHiveJobExecutorController( container, jobEntry, bindingFactory );
  }
}
