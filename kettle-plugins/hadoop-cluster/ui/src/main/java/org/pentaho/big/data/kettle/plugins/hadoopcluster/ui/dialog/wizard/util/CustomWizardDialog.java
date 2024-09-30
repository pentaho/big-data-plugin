/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2024 by Hitachi Vantara : http://www.pentaho.com
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

public class CustomWizardDialog extends WizardDialog {

  public CustomWizardDialog( Shell parentShell, IWizard newWizard ) {
    super( parentShell, newWizard );
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
