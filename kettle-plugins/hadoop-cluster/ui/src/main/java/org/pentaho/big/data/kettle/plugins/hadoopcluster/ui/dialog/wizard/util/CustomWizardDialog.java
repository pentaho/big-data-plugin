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

package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util;

import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.wizard.IWizard;
import org.eclipse.jface.wizard.WizardDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;

public class CustomWizardDialog extends WizardDialog {

  public CustomWizardDialog( Shell parentShell, IWizard newWizard ) {
    super( parentShell, newWizard );
    setDefaultImage( GUIResource.getInstance().getImageWizard() );
    setHelpAvailable( true );
    setShellStyle( SWT.CLOSE | SWT.TITLE | SWT.BORDER
      | SWT.APPLICATION_MODAL | getDefaultOrientation() );
    create();
    Rectangle shellBounds = getParentShell().getBounds();
    Point dialogSize = getShell().getSize();
    getShell().setLocation( shellBounds.x + ( shellBounds.width - dialogSize.x ) / 2,
      shellBounds.y + ( shellBounds.height - dialogSize.y ) / 2 );
  }

  public void style() {
    PropsUI propsUI = PropsUI.getInstance();
    propsUI.setLook( getButtonBar() );
    propsUI.setLook( getDialogArea() );
  }

  public void enableCancelButton( boolean isEnabled ) {
    Button cancelButton = getButton( IDialogConstants.CANCEL_ID );
    cancelButton.setEnabled( isEnabled );
  }
}
