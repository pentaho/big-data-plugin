/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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
