/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2002 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/


package org.pentaho.di.core.hadoop;

import java.util.List;

/**
 * Created by bryan on 8/13/15.
 */
public interface HadoopConfigurationPrompter {
  String getConfigurationSelection( List<HadoopConfigurationInfo> hadoopConfigurationInfos );

  void promptForRestart();
}
