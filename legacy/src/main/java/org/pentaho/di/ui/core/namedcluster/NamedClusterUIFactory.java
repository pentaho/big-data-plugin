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

import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.ui.spoon.Spoon;

/**
 * Created by bryan on 8/17/15.
 */
public interface NamedClusterUIFactory {
  NamedClusterWidget createNamedClusterWidget( Composite parent, boolean showLabel );

  HadoopClusterDelegate createHadoopClusterDelegate( Spoon spoon );

  NamedClusterDialog createNamedClusterDialog( Shell shell );

  NamedCluster getNamedClusterFromVfsFileChooser( Spoon spoon );
}
