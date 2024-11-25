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

package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.pages;

import org.eclipse.jface.wizard.WizardPage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CLabel;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.ExpandBar;
import org.eclipse.swt.widgets.ExpandItem;
import org.eclipse.swt.widgets.Label;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints.Test;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints.TestCategory;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.model.ThinNameClusterModel;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.util.HelpUtils;

import java.util.ArrayList;
import java.util.List;

import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.ONE_COLUMN;
import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.createLabelWithStyle;

public class TestResultsPage extends WizardPage {

  private PropsUI props;
  private Composite basePanel;
  private Composite parent;
  private Composite mainPanel;
  private ExpandBar testResultsExpandBar;
  private ThinNameClusterModel thinNameClusterModel;
  private static final Class<?> PKG = TestResultsPage.class;
  private static final String WARNING = BaseMessages.getString( PKG, "NamedClusterDialog.test.warning" );
  private static final String FAIL = BaseMessages.getString( PKG, "NamedClusterDialog.test.fail" );
  private static final String PASS = BaseMessages.getString( PKG, "NamedClusterDialog.test.pass" );
  private static final String WARNING_IMG = "images/warning_category.svg";
  private static final String FAIL_IMG = "images/fail_category.svg";
  private static final String PASS_IMG = "images/success_category.svg";
  private Composite testComposite;

  public TestResultsPage( VariableSpace variables, ThinNameClusterModel model ) {
    super( TestResultsPage.class.getSimpleName() );
    thinNameClusterModel = model;
  }

  public void createControl( Composite composite ) {
    parent = new Composite( composite, SWT.NONE );
    props = PropsUI.getInstance();
    props.setLook( parent );
    GridLayout gridLayout = new GridLayout( ONE_COLUMN, false );
    parent.setLayout( gridLayout );
    basePanel = new Composite( parent, SWT.NONE );

    //START OF MAIN LAYOUT
    GridLayout baseGridLayout = new GridLayout( ONE_COLUMN, false );
    baseGridLayout.marginWidth = 60; //TO CENTER CONTENTS
    baseGridLayout.marginTop = 10; //TO CENTER CONTENTS
    baseGridLayout.marginBottom = 30;
    baseGridLayout.marginLeft = 20;
    basePanel.setLayout( baseGridLayout );
    GridData basePanelGridData = new GridData( SWT.FILL, SWT.FILL, false, false );
    basePanel.setLayoutData( basePanelGridData );
    props.setLook( basePanel );
    //END OF MAIN LAYOUT

    mainPanel = new Composite( basePanel, SWT.NONE );
    mainPanel.setLayout( new GridLayout( ONE_COLUMN, false ) );
    GridData mainPanelGridData = new GridData( SWT.FILL, SWT.FILL, false, false );
    mainPanelGridData.heightHint = 510; //Height of the panel (WILL NEED TO ADJUST)
    mainPanel.setLayoutData( mainPanelGridData );
    props.setLook( mainPanel );

    GridData statusGridData = new GridData();
    statusGridData.widthHint = 400; // Label width
    statusGridData.heightHint = 50; // Label height
    Label statusLabel =
      createLabelWithStyle( mainPanel, BaseMessages.getString( PKG, "NamedClusterDialog.testResults" ), statusGridData,
        props, SWT.NONE );
    statusLabel.setFont( new Font( statusLabel.getDisplay(), new FontData( "Arial", 20, SWT.NONE ) ) );
    statusLabel.setAlignment( SWT.CENTER );

    testResultsExpandBar = new ExpandBar( mainPanel, SWT.V_SCROLL );
    GridData testResultsExpandBarLayoutData = new GridData( SWT.FILL, SWT.FILL, false, false );
    testResultsExpandBarLayoutData.heightHint = 400; //Height of the panel (WILL NEED TO ADJUST)
    testResultsExpandBarLayoutData.widthHint = 400; //Height of the panel (WILL NEED TO ADJUST)
    testResultsExpandBar.setLayoutData( testResultsExpandBarLayoutData );
    testResultsExpandBar.setSpacing( 8 );
    props.setLook( testResultsExpandBar );

    setControl( parent );
    initialize( thinNameClusterModel );
  }

  private List<TestCategory> setTestResultsOrder( Object[] categories ) {
    List<TestCategory> testCategories = new ArrayList<>();
    String[] categoryNames = new String[ 5 ];
    if ( Const.isWindows() || Const.isOSX() ) {
      categoryNames[ 0 ] = "Kafka";
      categoryNames[ 1 ] = "Oozie";
      categoryNames[ 2 ] = "Job";
      categoryNames[ 3 ] = "Zookeeper";
      categoryNames[ 4 ] = "Hadoop";
    } else {
      categoryNames[ 0 ] = "Hadoop";
      categoryNames[ 1 ] = "Zookeeper";
      categoryNames[ 2 ] = "Job";
      categoryNames[ 3 ] = "Oozie";
      categoryNames[ 4 ] = "Kafka";
    }
    for ( String categoryName : categoryNames ) {
      TestCategory category = getTestCategory( categoryName, categories );
      if ( category != null ) {
        testCategories.add( category );
      }
    }
    return testCategories;
  }

  private TestCategory getTestCategory( String categoryName, Object[] categories ) {
    TestCategory testCategory = null;
    for ( Object category : categories ) {
      if ( ( (TestCategory) category ).getCategoryName().startsWith( categoryName ) ) {
        testCategory = (TestCategory) category;
      }
    }
    return testCategory;
  }

  public void setTestResults( Object[] categories ) {
    for ( ExpandItem item : testResultsExpandBar.getItems() ) {
      item.dispose();
    }
    List<TestCategory> testCategories = setTestResultsOrder( categories );
    for ( TestCategory testCategory : testCategories ) {
      ExpandItem categoryItem = new ExpandItem( testResultsExpandBar, SWT.NONE, 0 );
      categoryItem.setText( testCategory.getCategoryName() );
      if ( testCategory.getCategoryStatus().equals( FAIL ) ) {
        categoryItem.setImage(
          GUIResource.getInstance().getImage( FAIL_IMG, getClass().getClassLoader(), 16, 16 ) );
      } else if ( testCategory.getCategoryStatus().isEmpty() ) {
        categoryItem.setImage(
          GUIResource.getInstance().getImage( WARNING_IMG, getClass().getClassLoader(), 16, 16 ) );
        categoryItem.setText( testCategory.getCategoryName() + " (skipped)" );
      } else if ( testCategory.getCategoryStatus().equals( WARNING ) ) {
        categoryItem.setImage(
          GUIResource.getInstance().getImage( WARNING_IMG, getClass().getClassLoader(), 16, 16 ) );
      } else if ( testCategory.getCategoryStatus().equals( PASS ) ) {
        categoryItem.setImage(
          GUIResource.getInstance().getImage( PASS_IMG, getClass().getClassLoader(), 16, 16 ) );
      }
      List<Test> tests = testCategory.getTests();
      if ( testComposite != null ) {
        testComposite.dispose();
      }
      testComposite = new Composite( testResultsExpandBar, SWT.NONE );
      props.setLook( testComposite );
      for ( Test test : tests ) {
        GridLayout testLayout = new GridLayout();
        testLayout.marginLeft = testLayout.marginTop = testLayout.marginRight = testLayout.marginBottom = 10;
        testLayout.verticalSpacing = 10;
        testComposite.setLayout( testLayout );
        GridData testLayoutData = new GridData();
        testLayoutData.widthHint = 400;
        testLayoutData.heightHint = 400;
        testComposite.setLayoutData( testLayoutData );
        CLabel testLabel = new CLabel( testComposite, SWT.NONE );
        if ( test.getTestStatus().equals( WARNING ) ) {
          testLabel.setImage(
            GUIResource.getInstance().getImage( WARNING_IMG, getClass().getClassLoader(), 16, 16 ) );
        } else if ( test.getTestStatus().equals( FAIL ) ) {
          testLabel.setImage(
            GUIResource.getInstance().getImage( FAIL_IMG, getClass().getClassLoader(), 16, 16 ) );
        } else if ( test.getTestStatus().equals( PASS ) ) {
          testLabel.setImage(
            GUIResource.getInstance().getImage( PASS_IMG, getClass().getClassLoader(), 16, 16 ) );
        }
        testLabel.setText( test.getTestName() );
        props.setLook( testLabel );
      }
      categoryItem.setHeight( testComposite.computeSize( SWT.DEFAULT, SWT.DEFAULT ).y );
      categoryItem.setControl( testComposite );
    }
  }

  public void initialize( ThinNameClusterModel model ) {
    thinNameClusterModel = model;
  }

  public void performHelp() {
    HelpUtils.openHelpDialog( parent.getShell(), "",
      BaseMessages.getString( PKG, "NamedClusterDialog.testResults.help" ), "" );
  }
}
