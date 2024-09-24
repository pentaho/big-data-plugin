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

package org.pentaho.di.ui.core.namedcluster;

import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.widgets.Composite;
import org.pentaho.di.core.namedcluster.model.NamedCluster;

/**
 * Created by bryan on 8/17/15.
 */
public interface NamedClusterWidget {
  void initiate();

  Composite getComposite();

  NamedCluster getSelectedNamedCluster();

  void addSelectionListener( SelectionListener selectionListener );

  void setSelectedNamedCluster( String name );
}
