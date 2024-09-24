/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.amazon.emr.ui;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.amazon.AbstractAmazonJobEntry;
import org.pentaho.amazon.AbstractAmazonJobEntryDialog;
import org.pentaho.amazon.AbstractAmazonJobExecutorController;
import org.pentaho.amazon.emr.job.AmazonElasticMapReduceJobExecutor;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.BindingFactory;

import java.lang.reflect.InvocationTargetException;

public class AmazonElasticMapReduceJobExecutorDialog extends AbstractAmazonJobEntryDialog {

  public AmazonElasticMapReduceJobExecutorDialog( Shell parent, JobEntryInterface jobEntry, Repository rep,
                                                  JobMeta jobMeta )
    throws XulException, InvocationTargetException {
    super( parent, jobEntry, rep, jobMeta );
  }

  @Override
  protected String getXulFile() {
    return "org/pentaho/amazon/emr/ui/AmazonElasticMapReduceJobExecutorDialog.xul";
  }

  @Override
  protected Class<?> getMessagesClass() {
    return AmazonElasticMapReduceJobExecutor.class;
  }

  @Override
  protected AbstractAmazonJobExecutorController createController( XulDomContainer container, AbstractAmazonJobEntry
    jobEntry,
                                                                  BindingFactory bindingFactory ) {
    return new AmazonElasticMapReduceJobExecutorController( container, jobEntry, bindingFactory );
  }
}
