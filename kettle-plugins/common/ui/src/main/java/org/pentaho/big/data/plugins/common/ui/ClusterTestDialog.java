/*******************************************************************************
 * Pentaho Big Data
 * <p/>
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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
import org.eclipse.swt.graphics.Rectangle;
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
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
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

  private RuntimeTestStatus runtimeTestStatus = null;

  /**
   * The log channel for this dialog.
   */
  protected LogChannelInterface log;

  public static ClusterTestDialog create( Shell parent, NamedCluster namedCluster, RuntimeTester clusterTester )
    throws KettleException {
    return new ClusterTestDialog( parent, namedCluster, clusterTester );
  }

  public ClusterTestDialog( Shell parent, NamedCluster namedCluster, RuntimeTester runtimeTester )
    throws KettleException {
    super( parent );
    this.namedCluster = namedCluster;
    this.runtimeTester = runtimeTester;
    props = getPropsUIInstance();
    this.log = KettleLogStore.getLogChannelInterfaceFactory().create( namedCluster );
  }

  /**
   * For testing
   */
  protected PropsUI getPropsUIInstance() {
    return PropsUI.getInstance();
  }

  public RuntimeTestStatus open() {
    Shell parent = getParent();
    final Display display = parent.getDisplay();
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.CLOSE | SWT.MAX | SWT.MIN | SWT.ICON );
    props.setLook( shell );
    shell.setImage( GUIResource.getInstance().getImageSpoon() );

    int margin = Const.FORM_MARGIN;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = margin;
    formLayout.marginHeight = margin;

    final int shellWidth = 385;
    final int shellHeight = 160;
    shell.setSize( shellWidth, shellHeight );
    shell.setMinimumSize( shellWidth, shellHeight );
    shell.setText( BaseMessages.getString( PKG, "ClusterTestDialog.Title" ) );
    shell.setLayout( formLayout );

    Label testingClusterLabel = new Label( shell, SWT.NONE );
    testingClusterLabel.setText( BaseMessages.getString( PKG, "ClusterTestDialog.ClusterTest.Label" ) );
    testingClusterLabel.setForeground( GUIResource.getInstance().getColorCrystalTextPentaho() );
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
    fd.right = new FormAttachment( 100, -margin );
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
    shell.setBackgroundMode( SWT.INHERIT_FORCE );

    Rectangle shellBounds = Spoon.getInstance().getShell().getBounds();

    shell.open();

    shell.setLocation(
      shellBounds.x + ( shellBounds.width - shellWidth ) / 2,
      shellBounds.y + ( shellBounds.height - shellHeight ) / 2 );

    // Start the cluster tests
    runtimeTester.runtimeTest( namedCluster, new RuntimeTestProgressCallback() {

      private int numTests = -1;

      @Override
      public void onProgress( final RuntimeTestStatus clusterTestStatus ) {
        Runnable runnable = getRunnable( progressBar, clusterTestStatus, testLabel );
        display.asyncExec( runnable );
      }
    } );

    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return runtimeTestStatus;
  }

  private void cancel() {
    runtimeTestStatus = null;
    dispose();
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }


  /**
   * For testing
   */
  Runnable getRunnable( final ProgressBar progressBar, final RuntimeTestStatus clusterTestStatus, final Label testLabel ) {
    return new Runnable() {
      private int numTests = -1;

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
          runtimeTestStatus = clusterTestStatus;
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

                if ( entry.getException() != null ) {
                  log.logBasic( ExceptionUtils.getStackTrace( entry.getException() ) );
                }
              } else {
                for ( RuntimeTestResultEntry entry : result.getRuntimeTestResultEntries() ) {
                  log.logBasic( BaseMessages.getString( PKG, "ClusterTestDialog.TestResult",
                          clusterTestName,
                          entry.getSeverity().toString(),
                          entry.getDescription() ) );
                  log.logBasic( "\t" + entry.getMessage() );

                  if ( entry.getException() != null ) {
                    log.logBasic( ExceptionUtils.getStackTrace( entry.getException() ) );
                  }
                }
              }
            }
          }
          ClusterTestDialog.this.dispose();
        }
      }
    };
  }

}
