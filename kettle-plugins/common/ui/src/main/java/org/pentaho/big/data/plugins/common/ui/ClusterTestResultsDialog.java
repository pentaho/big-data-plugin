/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.big.data.plugins.common.ui;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.util.HelpUtils;
import org.pentaho.runtime.test.RuntimeTestStatus;
import org.pentaho.runtime.test.action.RuntimeTestAction;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.pentaho.runtime.test.module.RuntimeTestModuleResults;
import org.pentaho.runtime.test.result.RuntimeTestResult;
import org.pentaho.runtime.test.result.RuntimeTestResultEntry;

/**
 * Dialog to display the results of running a suite of tests on a Named Cluster (and its shim/config)
 */
public class ClusterTestResultsDialog extends Dialog {

  private static final Class<?> PKG = ClusterTestResultsDialog.class;

  private Shell shell;
  private PropsUI props;

  private final RuntimeTestActionService runtimeTestActionService;
  private final RuntimeTestStatus clusterTestStatus;

  /**
   * The log channel for this dialog.
   */
  protected LogChannel log;

  public ClusterTestResultsDialog( Shell parent, RuntimeTestActionService runtimeTestActionService,
                                   RuntimeTestStatus clusterTestStatus )
    throws KettleException {
    super( parent );
    this.runtimeTestActionService = runtimeTestActionService;
    this.clusterTestStatus = clusterTestStatus;
    props = PropsUI.getInstance();
    this.log = new LogChannel( clusterTestStatus );
  }

  public String open() {
    Shell parent = getParent();
    final Display display = parent.getDisplay();
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.CLOSE | SWT.MAX | SWT.MIN | SWT.ICON );
    props.setLook( shell );
    shell.setImage( GUIResource.getInstance().getImageSpoon() );

    int margin = Const.FORM_MARGIN;

    HelpUtils.createHelpButton( shell, BaseMessages.getString( PKG, "ClusterTestResultsDialog.Shell.Doc.Title" ),
      BaseMessages.getString( PKG, "ClusterTestResultsDialog.Shell.Doc" ),
      BaseMessages.getString( PKG, "ClusterTestResultsDialog.Shell.Doc.Header" ) );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = margin;
    formLayout.marginHeight = margin;

    final int shellWidth = 585;
    final int shellHeight = 490;
    shell.setSize( shellWidth, shellHeight );
    shell.setMinimumSize( shellWidth, shellHeight );

    shell.setText( BaseMessages.getString( PKG, "ClusterTestResultsDialog.Title" ) );
    shell.setLayout( formLayout );
    shell.setBackgroundMode( SWT.INHERIT_FORCE );

    Label clusterResultsLabel = new Label( shell, SWT.NONE );
    clusterResultsLabel.setText( BaseMessages.getString( PKG, "ClusterTestResultsDialog.ClusterTestResults.Label" ) );
    clusterResultsLabel.setForeground( GUIResource.getInstance().getColorCrystalTextPentaho() );
    FormData fd = new FormData();
    fd.left = new FormAttachment( 0, margin );
    fd.top = new FormAttachment( 0, margin );
    fd.right = new FormAttachment( 100, -margin );
    clusterResultsLabel.setLayoutData( fd );

    final ScrolledComposite scrolledComposite = new ScrolledComposite( shell, SWT.V_SCROLL | SWT.BORDER );
    fd = new FormData();
    fd.left = new FormAttachment( 0, margin );
    fd.right = new FormAttachment( 100, -margin );
    fd.bottom = new FormAttachment( 100, -50 );
    fd.top = new FormAttachment( clusterResultsLabel, margin );
    scrolledComposite.setLayoutData( fd );

    final Composite mainComposite = new Composite( scrolledComposite, SWT.NONE );
    scrolledComposite.setContent( mainComposite );
    GridLayout layout = new GridLayout();
    layout.numColumns = 2;
    mainComposite.setLayout( layout );

    // Add the test results
    for ( RuntimeTestModuleResults moduleResults : clusterTestStatus.getModuleResults() ) {
      for ( RuntimeTestResult testResult : moduleResults.getRuntimeTestResults() ) {
        RuntimeTestResultEntry summary = testResult.getOverallStatusEntry();
        Label image = new Label( mainComposite, SWT.NONE );
        switch ( summary.getSeverity() ) {

          case DEBUG:
          case INFO:
            // The above are "Test(s) passed"
            image.setImage( GUIResource.getInstance().getImageTrue() );
            break;
          case WARNING:
          case SKIPPED:
            // The above are "Test(s) finished with warnings"
            image.setImage( GUIResource.getInstance().getImageWarning() );
            break;
          case ERROR:
          case FATAL:
            // The above are "Test(s) failed"
            image.setImage( GUIResource.getInstance().getImageFalse() );
            break;
        }
        // Need 2-row 1-col grid to display test name then description
        GridLayout testDescriptionLayout = new GridLayout();
        testDescriptionLayout.numColumns = 1;
        testDescriptionLayout.verticalSpacing = 0;
        testDescriptionLayout.horizontalSpacing = 0;

        final Composite testDescriptionComposite = new Composite( mainComposite, SWT.NONE );
        testDescriptionComposite.setLayout( testDescriptionLayout );

        Label testName = new Label( testDescriptionComposite, SWT.NONE );
        testName.setText( testResult.getRuntimeTest().getName() );
        GridData gridData = new GridData( SWT.FILL, SWT.FILL, true, true );
        testName.setLayoutData( gridData );

        // The Test description will also contain an "action" link
        GridLayout testResultsLayout = new GridLayout();
        testResultsLayout.numColumns = 2;
        testResultsLayout.marginHeight = 0;
        testResultsLayout.marginWidth = 0;
        final Composite testResultsComposite = new Composite( testDescriptionComposite, SWT.NONE );
        testResultsComposite.setLayout( testResultsLayout );

        // Add test description
        Label description = new Label( testResultsComposite, SWT.NONE );
        description.setForeground( GUIResource.getInstance().getColorDarkGray() );
        description.setText( summary.getDescription() );
        gridData = new GridData( SWT.LEFT, SWT.FILL, true, true );
        description.setLayoutData( gridData );

        // Add action link
        Link link = new Link( testResultsComposite, SWT.NONE );
        final RuntimeTestAction runtimeTestAction = summary.getAction();
        if ( runtimeTestAction != null ) {
          link.setText( "<a>" + runtimeTestAction.getName() + "</a>" );
          link.setToolTipText( runtimeTestAction.getDescription() );
          link.addSelectionListener( new SelectionAdapter() {
            public void widgetSelected( SelectionEvent selectionEvent ) {
              runtimeTestActionService.handle( runtimeTestAction );
            }

            /*public void widgetDefaultSelected( SelectionEvent selectionEvent ) {
              invoke( onclick );
            }*/
          } );
        }

        testResultsComposite.setSize( testResultsComposite.computeSize( SWT.DEFAULT, SWT.DEFAULT ) );

        testDescriptionComposite.setSize( testDescriptionComposite.computeSize( SWT.DEFAULT, SWT.DEFAULT ) );
      }
    }
    mainComposite.setSize( mainComposite.computeSize( SWT.DEFAULT, SWT.DEFAULT ) );

    Button wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wOk.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    } );

    Button[] buttons = new Button[]{ wOk };
    BaseStepDialog.positionBottomRightButtons( shell, buttons, margin, null );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return null;
  }

  private void ok() {
    dispose();
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

}
