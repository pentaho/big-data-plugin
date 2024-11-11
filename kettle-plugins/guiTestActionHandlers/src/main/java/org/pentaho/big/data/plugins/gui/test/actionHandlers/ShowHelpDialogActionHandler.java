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


package org.pentaho.big.data.plugins.gui.test.actionHandlers;

import org.eclipse.swt.widgets.Display;
import org.pentaho.di.ui.core.dialog.ShowHelpDialog;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.runtime.test.action.RuntimeTestAction;
import org.pentaho.runtime.test.action.RuntimeTestActionHandler;
import org.pentaho.runtime.test.action.impl.HelpUrlPayload;

/**
 * Created by bryan on 9/9/15.
 */
public class ShowHelpDialogActionHandler implements RuntimeTestActionHandler {

  @Override public boolean canHandle( RuntimeTestAction runtimeTestAction ) {
    return runtimeTestAction.getPayload() instanceof HelpUrlPayload;
  }

  @Override public void handle( RuntimeTestAction runtimeTestAction ) {
    // Cast checked in canHandle()
    final HelpUrlPayload helpUrlPayload = (HelpUrlPayload) runtimeTestAction.getPayload();
    final Spoon spoon = Spoon.getInstance();
    Display display = spoon.getDisplay();
    Runnable showRunnable = new Runnable() {
      @Override public void run() {
        new ShowHelpDialog( spoon.getShell(),
          helpUrlPayload.getTitle(),
          helpUrlPayload.getUrl().toString(),
          helpUrlPayload.getHeader() ).open();
      }
    };
    if ( Thread.currentThread() == display.getThread() ) {
      showRunnable.run();
    } else {
      display.asyncExec( showRunnable );
    }
  }
}
