/*******************************************************************************
 * Pentaho Big Data
 * <p/>
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 * <p/>
 * ******************************************************************************
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.runtime.test.RuntimeTest;
import org.pentaho.runtime.test.RuntimeTestProgressCallback;
import org.pentaho.runtime.test.RuntimeTestStatus;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.module.RuntimeTestModuleResults;
import org.pentaho.runtime.test.result.RuntimeTestResult;
import org.pentaho.runtime.test.result.RuntimeTestResultEntry;

import java.util.Iterator;


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
  private final RuntimeTester runtimeTester;

  /**
   * The log channel for this dialog.
   */
  protected LogChannel log;

  public static ClusterTestDialog create( Shell parent, NamedCluster namedCluster, RuntimeTester clusterTester )
    throws KettleException {
    return new ClusterTestDialog( parent, namedCluster, clusterTester );
  }

  public ClusterTestDialog( Shell parent, NamedCluster namedCluster, RuntimeTester runtimeTester )
    throws KettleException {
    super( parent );
    this.namedCluster = namedCluster;
    this.runtimeTester = runtimeTester;
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

    final Label testLabel = new Label( shell, SWT.NONE );
    testLabel.setText( "Testing cluster..." );
    fd = new FormData();
    fd.top = new FormAttachment( testingClusterLabel, 10 );
    fd.right = new FormAttachment( 100, -margin );
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
    runtimeTester.runtimeTest( namedCluster, new RuntimeTestProgressCallback() {

      private int numTests = -1;

      @Override
      public void onProgress( final RuntimeTestStatus clusterTestStatus ) {
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

            for ( RuntimeTestModuleResults results : clusterTestStatus.getModuleResults() ) {
              Iterator<RuntimeTest> runningTests = results.getRunningTests().iterator();
              if ( runningTests.hasNext() ) {
                testLabel.setText( runningTests.next().getName() );
              }
            }

            if ( clusterTestStatus.isDone() ) {
              testLabel.setText( BaseMessages.getString( PKG, "ClusterTestDialog.TestsFinished" ) );
              // Log all the executed tests at the end
              for ( RuntimeTestModuleResults results : clusterTestStatus.getModuleResults() ) {
                log.logBasic( BaseMessages.getString( PKG, "ClusterTestDialog.ModuleTest", results.getName() ) );
                for ( RuntimeTestResult result : results.getRuntimeTestResults() ) {
                  String clusterTestName = result.getRuntimeTest().getName();
                  // If there are no entries, that means there was one test and it becomes the summary-level result
                  if ( result.getRuntimeTestResultEntries().isEmpty() ) {
                    RuntimeTestResultEntry entry = result.getOverallStatusEntry();
                    log.logBasic( BaseMessages.getString( PKG, "ClusterTestDialog.TestResult",
                      clusterTestName,
                      entry.getSeverity().toString(),
                      entry.getDescription() ) );
                    log.logBasic( "\t" + entry.getMessage() );

                    if ( log.isDetailed() && entry.getException() != null ) {
                      log.logDetailed( ExceptionUtils.getStackTrace( entry.getException() ) );
                    }
                  } else {
                    for ( RuntimeTestResultEntry entry : result.getRuntimeTestResultEntries() ) {
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
              }

              try {
                new ClusterTestResultsDialog( shell, clusterTestStatus ).open();
              } catch ( KettleException ke ) {
                log.logError( BaseMessages.getString( PKG, "ClusterTestResultsDialog.FailedToOpen" ), ke );
              }
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
