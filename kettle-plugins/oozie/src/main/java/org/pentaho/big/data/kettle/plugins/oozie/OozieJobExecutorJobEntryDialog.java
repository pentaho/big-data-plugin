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


package org.pentaho.big.data.kettle.plugins.oozie;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.big.data.plugins.common.ui.HadoopClusterDelegateImpl;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.database.dialog.tags.ExtTextbox;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.XulSpoonSettingsManager;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.XulRunner;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.binding.DefaultBindingFactory;
import org.pentaho.ui.xul.swt.SwtXulLoader;
import org.pentaho.ui.xul.swt.SwtXulRunner;

import java.util.Enumeration;
import java.util.ResourceBundle;

/**
 * User: RFellows Date: 6/4/12
 */
public class OozieJobExecutorJobEntryDialog extends JobEntryDialog implements JobEntryDialogInterface {

  private static final String OOZIE_JOB_EXECUTOR_XUL = "org/pentaho/big/data/kettle/plugins/oozie/xul/OozieJobExecutor.xul";
  public static final String VARIABLETEXTBOX = "VARIABLETEXTBOX";
  public static final String LABEL = "LABEL";

  private OozieJobExecutorJobEntryController controller = null;
  private XulDomContainer container = null;

  public OozieJobExecutorJobEntryDialog( Shell parent, JobEntryInterface jobEntry, Repository rep, JobMeta jobMeta )
    throws XulException {
    super( parent, jobEntry, rep, jobMeta );
    init( OozieJobExecutorJobEntry.class.cast( jobEntry ) );
  }

  protected void init( OozieJobExecutorJobEntry jobEntry ) throws XulException {
    SwtXulLoader xulLoader = new SwtXulLoader();
    xulLoader.setSettingsManager( XulSpoonSettingsManager.getInstance() );
    xulLoader.registerClassLoader( getClass().getClassLoader() );

    // register the variable-aware text, LinkLabel, and a better checkbox box for use in XUL
    xulLoader.register( VARIABLETEXTBOX, ExtTextbox.class.getName() );
    xulLoader.setOuterContext( shell );

    // Load the XUL document with the dialog defined in it
    container = xulLoader.loadXul( getXulFile(), bundle );

    BindingFactory bf = new DefaultBindingFactory();
    bf.setDocument( container.getDocumentRoot() );
    controller = createController( jobEntry, container, bf );
    controller.setJobMeta( jobMeta );

    String clusterName = controller.getConfig().getClusterName();

    container.addEventHandler( controller );

    // Load up the SWT-XUL runtime and initialize it with our container
    final XulRunner runner = new SwtXulRunner();
    runner.addContainer( container );
    runner.initialize();

    controller.selectNamedCluster( clusterName );
  }

  protected OozieJobExecutorJobEntryController createController( OozieJobExecutorJobEntry jobEntry,
      XulDomContainer container, BindingFactory bindingFactory ) {
    return new OozieJobExecutorJobEntryController( jobMeta, container, jobEntry, bindingFactory,
      new HadoopClusterDelegateImpl( Spoon.getInstance(), jobEntry.getNamedClusterService(),
        jobEntry.getRuntimeTestActionService(), jobEntry.getRuntimeTester() ) );
  }

  private String getMessage( String key ) {
    return BaseMessages.getString( OozieJobExecutorJobEntry.class, key );
  }

  @Override
  public JobEntryInterface open() {
    return controller.open();
  }

  protected String getXulFile() {
    return OOZIE_JOB_EXECUTOR_XUL;
  }

  protected ResourceBundle bundle = new ResourceBundle() {
    @Override
    protected Object handleGetObject( String key ) {
      return BaseMessages.getString( OozieJobExecutorJobEntry.class, key );
    }

    @Override
    public Enumeration<String> getKeys() {
      return null;
    }
  };
}
