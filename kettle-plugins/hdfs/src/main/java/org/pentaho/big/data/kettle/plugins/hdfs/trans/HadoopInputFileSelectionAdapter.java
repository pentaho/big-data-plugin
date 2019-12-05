/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.big.data.kettle.plugins.hdfs.trans;

import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.ui.core.events.dialog.SelectionAdapterFileDialog;
import org.pentaho.di.ui.core.events.dialog.SelectionAdapterOptions;
import org.pentaho.di.ui.core.widget.TableView;

public class HadoopInputFileSelectionAdapter extends SelectionAdapterFileDialog<TableView> {

  public HadoopInputFileSelectionAdapter( LogChannelInterface log, TableView textUiWidget, AbstractMeta meta,
                                          SelectionAdapterOptions options ) {
    super( log, textUiWidget, meta, options );
  }

  @Override
  protected String getText() {
    return this.getTextWidget().getActiveTableItem().getText( this.getTextWidget().getActiveTableColumn() );
  }

  @Override
  protected void setText( String text ) {
    this.getTextWidget().getActiveTableItem().setText( this.getTextWidget().getActiveTableColumn(), text );
  }
}
