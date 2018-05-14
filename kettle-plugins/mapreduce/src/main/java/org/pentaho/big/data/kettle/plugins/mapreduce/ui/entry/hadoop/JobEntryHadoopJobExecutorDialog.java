/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.mapreduce.ui.entry.hadoop;

import org.dom4j.DocumentException;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.big.data.kettle.plugins.mapreduce.entry.hadoop.JobEntryHadoopJobExecutor;
import org.pentaho.big.data.plugins.common.ui.HadoopClusterDelegateImpl;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.XulRunner;
import org.pentaho.ui.xul.binding.Binding.Type;
import org.pentaho.ui.xul.binding.BindingConvertor;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.binding.DefaultBindingFactory;
import org.pentaho.ui.xul.components.XulMenuList;
import org.pentaho.ui.xul.components.XulRadio;
import org.pentaho.ui.xul.components.XulTextbox;
import org.pentaho.ui.xul.containers.XulDialog;
import org.pentaho.ui.xul.containers.XulTree;
import org.pentaho.ui.xul.containers.XulVbox;
import org.pentaho.ui.xul.swt.SwtXulLoader;
import org.pentaho.ui.xul.swt.SwtXulRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.ResourceBundle;

public class JobEntryHadoopJobExecutorDialog extends JobEntryDialog implements JobEntryDialogInterface {
  private static final Class<?> CLZ = JobEntryHadoopJobExecutor.class;
  private static final Logger logger = LoggerFactory.getLogger( JobEntryHadoopJobExecutorDialog.class );
  private final NamedClusterService namedClusterService;
  private final JobEntryHadoopJobExecutorController controller;
  private JobEntryHadoopJobExecutor jobEntry;
  private XulDomContainer container;

  private BindingFactory bf;

  private ResourceBundle bundle = new ResourceBundle() {
    @Override
    public Enumeration<String> getKeys() {
      return null;
    }

    @Override
    protected Object handleGetObject( String key ) {
      return BaseMessages.getString( CLZ, key );
    }
  };

  public JobEntryHadoopJobExecutorDialog( Shell parent, JobEntryInterface jobEntry, Repository rep, JobMeta jobMeta )
    throws XulException, DocumentException, Throwable {
    super( parent, jobEntry, rep, jobMeta );
    this.jobEntry = (JobEntryHadoopJobExecutor) jobEntry;
    this.namedClusterService = this.jobEntry.getNamedClusterService();
    controller = new JobEntryHadoopJobExecutorController(
      new HadoopClusterDelegateImpl( Spoon.getInstance(), namedClusterService,
        this.jobEntry.getRuntimeTestActionService(), this.jobEntry.getRuntimeTester() ), namedClusterService,
      this.jobEntry.getNamedClusterServiceLocator() );

    SwtXulLoader swtXulLoader = new SwtXulLoader();
    swtXulLoader.registerClassLoader( getClass().getClassLoader() );
    swtXulLoader.register( "VARIABLETEXTBOX", "org.pentaho.di.ui.core.database.dialog.tags.ExtTextbox" );
    swtXulLoader.register( "VARIABLEMENULIST", "org.pentaho.di.ui.core.database.dialog.tags.ExtMenuList" );
    swtXulLoader.setOuterContext( shell );

    container =
      swtXulLoader
        .loadXul( "org/pentaho/big/data/kettle/plugins/mapreduce/ui/entry/JobEntryHadoopJobExecutorDialog.xul",
        bundle ); //$NON-NLS-1$

    final XulRunner runner = new SwtXulRunner();
    runner.addContainer( container );

    container.addEventHandler( controller );

    bf = new DefaultBindingFactory();
    bf.setDocument( container.getDocumentRoot() );
    bf.setBindingType( Type.BI_DIRECTIONAL );

    bf.createBinding( "jobentry-name", "value", controller,
      JobEntryHadoopJobExecutorController.JOB_ENTRY_NAME ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

    bf.createBinding( "jobentry-hadoopjob-name", "value", controller,
      JobEntryHadoopJobExecutorController.HADOOP_JOB_NAME ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    bf.createBinding( "jar-url", "value", controller,
      JobEntryHadoopJobExecutorController.JAR_URL ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    bf.createBinding( "driver-class", "value", controller,
      JobEntryHadoopJobExecutorController.DRIVER_CLASS ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    bf.createBinding( "driver-class", "selectedItem", controller,
      JobEntryHadoopJobExecutorController.DRIVER_CLASS ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    bf.createBinding( "driver-class", "elements", controller,
      JobEntryHadoopJobExecutorController.DRIVER_CLASSES ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    bf.createBinding( "command-line-arguments", "value", controller.getSimpleConfiguration(),
      JobEntryHadoopJobExecutorController.SimpleConfiguration.CMD_LINE_ARGS ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

    bf.createBinding( "classes-output-key-class", "value", controller.getAdvancedConfiguration(),
      JobEntryHadoopJobExecutorController.AdvancedConfiguration.OUTPUT_KEY_CLASS ); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding( "classes-output-value-class", "value", controller.getAdvancedConfiguration(),
      JobEntryHadoopJobExecutorController.AdvancedConfiguration.OUTPUT_VALUE_CLASS ); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding( "classes-mapper-class", "value", controller.getAdvancedConfiguration(),
      JobEntryHadoopJobExecutorController.AdvancedConfiguration.MAPPER_CLASS ); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding( "classes-combiner-class", "value", controller.getAdvancedConfiguration(),
      JobEntryHadoopJobExecutorController.AdvancedConfiguration.COMBINER_CLASS ); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding( "classes-reducer-class", "value", controller.getAdvancedConfiguration(),
      JobEntryHadoopJobExecutorController.AdvancedConfiguration.REDUCER_CLASS ); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding( "classes-input-format", "value", controller.getAdvancedConfiguration(),
      JobEntryHadoopJobExecutorController.AdvancedConfiguration.INPUT_FORMAT_CLASS ); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding( "classes-output-format", "value", controller.getAdvancedConfiguration(),
      JobEntryHadoopJobExecutorController.AdvancedConfiguration.OUTPUT_FORMAT_CLASS ); //$NON-NLS-1$ //$NON-NLS-2$

    // bf.createBinding("num-map-tasks", "value", controller.getAdvancedConfiguration(),
    // AdvancedConfiguration.NUM_MAP_TASKS, bindingConverter); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding( "num-map-tasks", "value", controller.getAdvancedConfiguration(),
      JobEntryHadoopJobExecutorController.AdvancedConfiguration.NUM_MAP_TASKS ); //$NON-NLS-1$ //$NON-NLS-2$
    // bf.createBinding("num-reduce-tasks", "value", controller.getAdvancedConfiguration(),
    // AdvancedConfiguration.NUM_REDUCE_TASKS, bindingConverter); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding( "num-reduce-tasks", "value", controller.getAdvancedConfiguration(),
      JobEntryHadoopJobExecutorController.AdvancedConfiguration.NUM_REDUCE_TASKS ); //$NON-NLS-1$ //$NON-NLS-2$

    bf.createBinding( "simple-blocking", "selected", controller.getSimpleConfiguration(),
      JobEntryHadoopJobExecutorController.SimpleConfiguration.BLOCKING ); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding( "blocking", "selected", controller.getAdvancedConfiguration(),
      JobEntryHadoopJobExecutorController.AdvancedConfiguration.BLOCKING ); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding( "simple-logging-interval", "value", controller.getSimpleConfiguration(),
      JobEntryHadoopJobExecutorController.SimpleConfiguration.LOGGING_INTERVAL ); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding( "logging-interval", "value", controller.getAdvancedConfiguration(),
      JobEntryHadoopJobExecutorController.AdvancedConfiguration.LOGGING_INTERVAL ); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding( "input-path", "value", controller.getAdvancedConfiguration(),
      JobEntryHadoopJobExecutorController.AdvancedConfiguration.INPUT_PATH ); //$NON-NLS-1$ //$NON-NLS-2$
    bf.createBinding( "output-path", "value", controller.getAdvancedConfiguration(),
      JobEntryHadoopJobExecutorController.AdvancedConfiguration.OUTPUT_PATH ); //$NON-NLS-1$ //$NON-NLS-2$

    ( (XulRadio) container.getDocumentRoot().getElementById( "simpleRadioButton" ) ).setSelected( this.jobEntry
      .isSimple() ); //$NON-NLS-1$
    ( (XulRadio) container.getDocumentRoot().getElementById( "advancedRadioButton" ) ).setSelected( !this.jobEntry
      .isSimple() ); //$NON-NLS-1$

    ( (XulVbox) container.getDocumentRoot().getElementById( "advanced-configuration" ) ).setVisible( !this.jobEntry
      .isSimple() ); //$NON-NLS-1$

    XulTextbox simpleLoggingInterval =
      (XulTextbox) container.getDocumentRoot().getElementById( "simple-logging-interval" );
    simpleLoggingInterval.setValue( "" + controller.getSimpleConfiguration().getSimpleLoggingInterval() );

    XulTextbox loggingInterval = (XulTextbox) container.getDocumentRoot().getElementById( "logging-interval" );
    loggingInterval.setValue( controller.getAdvancedConfiguration().getLoggingInterval() );

    XulTextbox mapTasks = (XulTextbox) container.getDocumentRoot().getElementById( "num-map-tasks" );
    mapTasks.setValue( controller.getAdvancedConfiguration().getNumMapTasks() );

    XulTextbox reduceTasks = (XulTextbox) container.getDocumentRoot().getElementById( "num-reduce-tasks" );
    reduceTasks.setValue( controller.getAdvancedConfiguration().getNumReduceTasks() );

    XulTree variablesTree = (XulTree) container.getDocumentRoot().getElementById( "fields-table" ); //$NON-NLS-1$
    bf.setBindingType( Type.ONE_WAY );
    bf.createBinding( controller.getUserDefined(), "children", variablesTree, "elements" ); //$NON-NLS-1$//$NON-NLS-2$
    bf.setBindingType( Type.BI_DIRECTIONAL );

    controller.setJobMeta( jobMeta );
    controller.setJobEntry( (JobEntryHadoopJobExecutor) jobEntry );
    controller.init();

    bf.setBindingType( Type.ONE_WAY );
    bf.createBinding( controller, "namedClusters", "named-clusters", "elements" ).fireSourceChanged();
    bf.setBindingType( Type.BI_DIRECTIONAL );
    bf.createBinding( "named-clusters", "selectedIndex", controller.getAdvancedConfiguration(), "selectedNamedCluster",
      new BindingConvertor<Integer, NamedCluster>() {
        public NamedCluster sourceToTarget( final Integer index ) {
          List<NamedCluster> clusters = new ArrayList<>();
          try {
            clusters = controller.getNamedClusters();
          } catch ( MetaStoreException e ) {
            logger.error( e.getMessage(), e );
          }
          if ( index == -1 || clusters.isEmpty() ) {
            return null;
          }
          return clusters.get( index );
        }

        public Integer targetToSource( final NamedCluster value ) {
          return null;
        }
      } ).fireSourceChanged();

    selectNamedCluster();
  }

  private void selectNamedCluster() {
    @SuppressWarnings( "unchecked" )
    XulMenuList<NamedCluster> namedClusterMenu =
        (XulMenuList<NamedCluster>) container.getDocumentRoot().getElementById( "named-clusters" ); //$NON-NLS-1$

    NamedCluster namedCluster = jobEntry.getNamedCluster();
    if ( namedCluster != null && isKnownNamedCluster( namedCluster, controller ) ) {
      namedClusterMenu.setSelectedItem( namedCluster );
      controller.getAdvancedConfiguration().setSelectedNamedCluster( namedCluster );
    }
  }

  public JobEntryInterface open() {
    XulDialog dialog = (XulDialog) container.getDocumentRoot().getElementById( "job-entry-dialog" ); //$NON-NLS-1$
    dialog.show();

    return jobEntry;
  }

  private boolean isKnownNamedCluster( NamedCluster jobNameCluster, JobEntryHadoopJobExecutorController controller ) {
    boolean result = false;
    if ( jobNameCluster != null ) {
      String jncName = jobNameCluster.getName();
      List<NamedCluster> nClusters = null;
      try {
        nClusters = controller.getNamedClusters();
      } catch ( MetaStoreException e ) {
        logger.error( e.getMessage(), e );
      }
      if ( jncName != null && nClusters != null ) {
        for ( NamedCluster nc : nClusters ) {
          if ( jncName != null && jncName.equals( nc.getName() ) ) {
            result = true;
            break;
          }
        }
      }
    }
    return result;
  }

}
