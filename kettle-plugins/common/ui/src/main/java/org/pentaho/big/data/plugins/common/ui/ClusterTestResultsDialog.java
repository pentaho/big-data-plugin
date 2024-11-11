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


package org.pentaho.big.data.plugins.common.ui;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
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
import org.pentaho.di.ui.spoon.Spoon;
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

    String docUrl = Const.getDocUrl( BaseMessages.getString( PKG, "ClusterTestResultsDialog.Shell.Doc" ) );
    HelpUtils.createHelpButton( shell,
        BaseMessages.getString( PKG, "ClusterTestResultsDialog.Shell.Doc.Title" ),
        docUrl,
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
    scrolledComposite.setExpandHorizontal( true );
    FormLayout layout = new FormLayout();
    mainComposite.setLayout( layout );

    ClassLoader myClassLoader = this.getClass().getClassLoader();

    Label separator = null;

    // Add the test results
    for ( RuntimeTestModuleResults moduleResults : clusterTestStatus.getModuleResults() ) {
      for ( RuntimeTestResult testResult : moduleResults.getRuntimeTestResults() ) {
        RuntimeTestResultEntry summary = testResult.getOverallStatusEntry();
        Label image = new Label( mainComposite, SWT.NONE );
        switch ( summary.getSeverity() ) {

          case DEBUG:
          case INFO:
            // The above are "Test(s) passed"
            image.setImage(
              GUIResource.getInstance().getImage( "ui/images/success_green.svg", myClassLoader, 22, 22 ) );
            break;
          case WARNING:
          case SKIPPED:
            // The above are "Test(s) finished with warnings"
            image.setImage(
              GUIResource.getInstance().getImage( "ui/images/warning_yellow.svg", myClassLoader, 22, 22 ) );
            break;
          case ERROR:
          case FATAL:
            // The above are "Test(s) failed"
            image.setImage(
              GUIResource.getInstance().getImage( "ui/images/error_red.svg", myClassLoader, 22, 22 ) );
            break;
        }
        FormData imageLayoutData = new FormData();
        imageLayoutData.left = new FormAttachment( 0, margin );
        if ( separator != null ) {
          imageLayoutData.top = new FormAttachment( separator, margin );
        } else {
          imageLayoutData.top = new FormAttachment( 0, margin );
        }
        image.setLayoutData( imageLayoutData );

        Label testName = new Label( mainComposite, SWT.NONE );
        testName.setText( testResult.getRuntimeTest().getName() );
        FormData layoutData = new FormData();
        layoutData.left = new FormAttachment( image, margin );
        layoutData.right = new FormAttachment( 100, -margin );
        if ( separator != null ) {
          layoutData.top = new FormAttachment( separator, margin );
        } else {
          layoutData.top = new FormAttachment( 0, margin );
        }
        testName.setLayoutData( layoutData );

        // Add test description
        Label description = new Label( mainComposite, SWT.WRAP );
        description.setForeground( GUIResource.getInstance().getColorDarkGray() );
        description.setText( summary.getDescription() );
        layoutData = new FormData();
        layoutData.left = new FormAttachment( image, margin );
        layoutData.right = new FormAttachment( 100, -margin );
        layoutData.top = new FormAttachment( testName, margin );
        description.setLayoutData( layoutData );

        Control linkOrNot = description;
        // Add action link(s)
        final RuntimeTestAction runtimeTestAction = summary.getAction();
        if ( runtimeTestAction != null ) {
          Link link = new Link( mainComposite, SWT.NONE );
          link.setText( "<a>" + runtimeTestAction.getName() + "</a>" );
          link.setToolTipText( runtimeTestAction.getDescription() );
          link.addSelectionListener( new SelectionAdapter() {
            public void widgetSelected( SelectionEvent selectionEvent ) {
              runtimeTestActionService.handle( runtimeTestAction );
            }
          } );
          layoutData = new FormData();
          layoutData.left = new FormAttachment( image, margin );
          layoutData.right = new FormAttachment( 100, -margin );
          layoutData.top = new FormAttachment( description, margin );
          link.setLayoutData( layoutData );
          linkOrNot = link;
        }

        // Add separator
        separator = new Label( mainComposite, SWT.HORIZONTAL | SWT.SEPARATOR );
        separator.setForeground( GUIResource.getInstance().getColorLightGray() );
        layoutData = new FormData();
        layoutData.left = new FormAttachment( 0, margin );
        layoutData.right = new FormAttachment( 100, -margin );
        layoutData.top = new FormAttachment( linkOrNot, margin );
        separator.setLayoutData( layoutData );
      }
    }
    mainComposite.setSize( mainComposite.computeSize( SWT.DEFAULT, SWT.DEFAULT ) );

    Button wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.Close" ) );

    wOk.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    } );

    Button[] buttons = new Button[]{ wOk };
    BaseStepDialog.positionBottomRightButtons( shell, buttons, margin, null );

    Rectangle shellBounds = Spoon.getInstance().getShell().getBounds();

    shell.pack();
    shell.open();

    shell.setLocation(
      shellBounds.x + ( shellBounds.width - shellWidth ) / 2,
      shellBounds.y + ( shellBounds.height - shellHeight ) / 2 );

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
