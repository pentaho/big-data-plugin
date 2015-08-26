/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.ProgressBar;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.ClusterTestProgressCallback;
import org.pentaho.big.data.api.clusterTest.ClusterTestStatus;
import org.pentaho.big.data.api.clusterTest.ClusterTester;
import org.pentaho.big.data.api.clusterTest.module.ClusterTestModuleResults;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResult;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.trans.step.BaseStepDialog;


/**
 * Dialog for testing a Named Cluster
 *
 * @see <code>NamedCluster</code>
 */
public class ClusterTestDialog extends Dialog {

  private static final Class<?> PKG = ClusterTestDialog.class;

  private Shell shell;
  private PropsUI props;

  private final NamedCluster namedCluster;
  private final ClusterTester clusterTester;

  /**
   * The log channel for this dialog.
   */
  protected LogChannel log;

  public static ClusterTestDialog create( Shell parent, NamedCluster namedCluster, ClusterTester clusterTester )
    throws KettleException {
    return new ClusterTestDialog( parent, namedCluster, clusterTester );
  }

  public ClusterTestDialog( Shell parent, NamedCluster namedCluster, ClusterTester clusterTester )
    throws KettleException {
    super( parent );
    this.namedCluster = namedCluster;
    this.clusterTester = clusterTester;
    props = PropsUI.getInstance();
    this.log = new LogChannel( namedCluster );
  }

  public String open() {
    Shell parent = getParent();
    final Display display = parent.getDisplay();
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.CLOSE | SWT.MAX | SWT.MIN | SWT.ICON );
    props.setLook( shell );
    shell.setImage( GUIResource.getInstance().getImageSpoon() );

    int margin = Const.FORM_MARGIN;

    /*PluginInterface plugin =
      PluginRegistry.getInstance().findPluginWithId( LifecyclePluginType.class, "HadoopSpoonPlugin" ); // TODO
    HelpUtils.createHelpButton( shell, HelpUtils.getHelpDialogTitle( plugin ),
      BaseMessages.getString( PKG, "ClusterTestDialog.Shell.Doc" ),
      BaseMessages.getString( PKG, "ClusterTestDialog.Shell.Title" ) );
*/
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = margin;
    formLayout.marginHeight = margin;

    final int shellWidth = 150;
    final int shellHeight = 243; // Golden ratio * width
    shell.setSize( shellHeight, shellWidth );
    shell.setMinimumSize( shellHeight, shellWidth );

    shell.setText( BaseMessages.getString( PKG, "ClusterTestDialog.Title" ) );
    shell.setLayout( formLayout );

    Label testingClusterLabel = new Label( shell, SWT.NONE );
    testingClusterLabel.setText( BaseMessages.getString( PKG, "ClusterTestDialog.ClusterTest.Label" ) );
    FontData[] fontData = testingClusterLabel.getFont().getFontData();
    fontData[0].setHeight( 16 );
    testingClusterLabel.setForeground( GUIResource.getInstance().getColorCrystalTextPentaho() );
    testingClusterLabel.setFont( new Font( display, fontData[0] ) );
    FormData fd = new FormData();
    fd.left = new FormAttachment( 0, margin );
    fd.top = new FormAttachment( 0, margin );
    testingClusterLabel.setLayoutData( fd );

    Label testLabel = new Label( shell, SWT.NONE );
    testLabel.setText( "Connecting to cluster" );
    fd = new FormData();
    fd.top = new FormAttachment( testingClusterLabel, 10 );
    fd.left = new FormAttachment( 0, margin );
    testLabel.setLayoutData( fd );

    final ProgressBar progressBar = new ProgressBar( shell, SWT.SMOOTH );
    progressBar.setMinimum( 0 ); // Max tests will be set upon first return
    fd = new FormData();
    fd.top = new FormAttachment( testLabel, 10 );
    fd.left = new FormAttachment( 0, margin );
    progressBar.setLayoutData( fd );

    Button wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    wCancel.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    } );

    Button[] buttons = new Button[]{ wCancel };
    BaseStepDialog.positionBottomRightButtons( shell, buttons, margin, null );

    shell.open();

    // Start the cluster tests
    clusterTester.testCluster( namedCluster, new ClusterTestProgressCallback() {

      private int numTests = -1;

      @Override
      public void onProgress( final ClusterTestStatus clusterTestStatus ) {
        display.asyncExec( new Runnable() {
          @Override
          public void run() {
            if ( progressBar.isDisposed() ) {
              return;
            }

            // Calculate the number of tests to be run (only the first time!)
            if ( numTests == -1 ) {
              numTests = clusterTestStatus.getTestsDone()
                + clusterTestStatus.getTestsOutstanding()
                + clusterTestStatus.getTestsRunning();

              progressBar.setMaximum( numTests );
            }

            progressBar.setSelection( clusterTestStatus.getTestsDone() );

            if ( clusterTestStatus.isDone() ) {
              // Log all the executed tests at the end
              for ( ClusterTestModuleResults results : clusterTestStatus.getModuleResults() ) {
                log.logBasic( BaseMessages.getString( PKG, "ClusterTestDialog.ModuleTest", results.getName() ) );
                for ( ClusterTestResult result : results.getClusterTestResults() ) {
                  String clusterTestName = result.getClusterTest().getName();
                  for ( ClusterTestResultEntry entry : result.getClusterTestResultEntries() ) {
                    log.logBasic( BaseMessages.getString( PKG, "ClusterTestDialog.TestResult",
                      clusterTestName,
                      entry.getSeverity().toString(),
                      entry.getDescription() ) );
                    log.logBasic( "\t" + entry.getMessage() );

                    if ( log.isDetailed() && entry.getException() != null ) {
                      log.logDetailed( ExceptionUtils.getStackTrace( entry.getException() ) );
                    }
                  }
                }
              }
              // TODO open Results dialog
              ClusterTestDialog.this.dispose();
            }
          }
        } );
      }
    } );

    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return null;
  }

  private void cancel() {
    dispose();
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }
}
