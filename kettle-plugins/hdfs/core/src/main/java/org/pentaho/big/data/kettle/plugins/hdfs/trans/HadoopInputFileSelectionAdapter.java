/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
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
