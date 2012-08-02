/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2012 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.job.entries.hadoopjobexecutor;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.ResultFile;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.hadoop.HadoopConfigurationRegistry;
import org.pentaho.di.core.logging.Log4jFileAppender;
import org.pentaho.di.core.logging.LogWriter;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.job.entries.hadoopjobexecutor.UserDefinedItem;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.fs.FileSystem;
import org.pentaho.hadoop.shim.api.fs.Path;
import org.pentaho.hadoop.shim.api.mapred.RunningJob;
import org.pentaho.hadoop.shim.api.mapred.TaskCompletionEvent;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.w3c.dom.Node;

@JobEntry(id = "HadoopJobExecutorPlugin", name = "Hadoop Job Executor", categoryDescription = "Big Data", description = "Execute MapReduce jobs in Hadoop", image = "HDE.png")
public class JobEntryHadoopJobExecutor extends JobEntryBase implements Cloneable, JobEntryInterface {

  private static Class<?> PKG = JobEntryHadoopJobExecutor.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

  private String hadoopJobName;

  private String jarUrl = "";

  private boolean isSimple = true;  

  private String cmdLineArgs;

  private String outputKeyClass;
  private String outputValueClass;
  private String mapperClass;
  private String combinerClass;
  private String reducerClass;
  private String inputFormatClass;
  private String outputFormatClass;

  private String workingDirectory;

  private String hdfsHostname;
  private String hdfsPort;

  private String jobTrackerHostname;
  private String jobTrackerPort;

  private String inputPath;
  private String outputPath;

  private boolean blocking;
  private String loggingInterval = "60"; // 60 seconds default

  private String numMapTasks = "1";
  private String numReduceTasks = "1";

  private List<UserDefinedItem> userDefined = new ArrayList<UserDefinedItem>();
  
  public String getHadoopJobName() {
    return hadoopJobName;
  }

  public void setHadoopJobName(String hadoopJobName) {
    this.hadoopJobName = hadoopJobName;
  }

  public String getJarUrl() {
    return jarUrl;
  }

  public void setJarUrl(String jarUrl) {
    this.jarUrl = jarUrl;
  }

  public boolean isSimple() {
    return isSimple;
  }

  public void setSimple(boolean isSimple) {
    this.isSimple = isSimple;
  }

  public String getCmdLineArgs() {
    return cmdLineArgs;
  }

  public void setCmdLineArgs(String cmdLineArgs) {
    this.cmdLineArgs = cmdLineArgs;
  }

  public String getOutputKeyClass() {
    return outputKeyClass;
  }

  public void setOutputKeyClass(String outputKeyClass) {
    this.outputKeyClass = outputKeyClass;
  }

  public String getOutputValueClass() {
    return outputValueClass;
  }

  public void setOutputValueClass(String outputValueClass) {
    this.outputValueClass = outputValueClass;
  }

  public String getMapperClass() {
    return mapperClass;
  }

  public void setMapperClass(String mapperClass) {
    this.mapperClass = mapperClass;
  }

  public String getCombinerClass() {
    return combinerClass;
  }

  public void setCombinerClass(String combinerClass) {
    this.combinerClass = combinerClass;
  }

  public String getReducerClass() {
    return reducerClass;
  }

  public void setReducerClass(String reducerClass) {
    this.reducerClass = reducerClass;
  }

  public String getInputFormatClass() {
    return inputFormatClass;
  }

  public void setInputFormatClass(String inputFormatClass) {
    this.inputFormatClass = inputFormatClass;
  }

  public String getOutputFormatClass() {
    return outputFormatClass;
  }

  public void setOutputFormatClass(String outputFormatClass) {
    this.outputFormatClass = outputFormatClass;
  }

  public String getWorkingDirectory() {
    return workingDirectory;
  }

  public void setWorkingDirectory(String workingDirectory) {
    this.workingDirectory = workingDirectory;
  }

  public String getHdfsHostname() {
    return hdfsHostname;
  }

  public void setHdfsHostname(String hdfsHostname) {
    this.hdfsHostname = hdfsHostname;
  }

  public String getHdfsPort() {
    return hdfsPort;
  }

  public void setHdfsPort(String hdfsPort) {
    this.hdfsPort = hdfsPort;
  }

  public String getJobTrackerHostname() {
    return jobTrackerHostname;
  }

  public void setJobTrackerHostname(String jobTrackerHostname) {
    this.jobTrackerHostname = jobTrackerHostname;
  }

  public String getJobTrackerPort() {
    return jobTrackerPort;
  }

  public void setJobTrackerPort(String jobTrackerPort) {
    this.jobTrackerPort = jobTrackerPort;
  }

  public String getInputPath() {
    return inputPath;
  }

  public void setInputPath(String inputPath) {
    this.inputPath = inputPath;
  }

  public String getOutputPath() {
    return outputPath;
  }

  public void setOutputPath(String outputPath) {
    this.outputPath = outputPath;
  }

  public boolean isBlocking() {
    return blocking;
  }

  public void setBlocking(boolean blocking) {
    this.blocking = blocking;
  }

  public String getLoggingInterval() {
    return loggingInterval;
  }

  public void setLoggingInterval(String loggingInterval) {
    this.loggingInterval = loggingInterval;
  }

  public List<UserDefinedItem> getUserDefined() {
    return userDefined;
  }

  public void setUserDefined(List<UserDefinedItem> userDefined) {
    this.userDefined = userDefined;
  }

  public String getNumMapTasks() {
    return numMapTasks;
  }

  public void setNumMapTasks(String numMapTasks) {
    this.numMapTasks = numMapTasks;
  }

  public String getNumReduceTasks() {
    return numReduceTasks;
  }

  public void setNumReduceTasks(String numReduceTasks) {
    this.numReduceTasks = numReduceTasks;
  }
  
  @SuppressWarnings("deprecation")
  public Result execute(Result result, int arg1) throws KettleException {
    result.setNrErrors(0);
    
    Log4jFileAppender appender = null;
    String logFileName = "pdi-" + this.getName(); //$NON-NLS-1$

    try
    {
      appender = LogWriter.createFileAppender(logFileName, true, false);
      LogWriter.getInstance().addAppender(appender);
      log.setLogLevel(parentJob.getLogLevel());
    } catch (Exception e)
    {
      logError(BaseMessages.getString(PKG, "JobEntryHadoopJobExecutor.FailedToOpenLogFile", logFileName, e.toString())); //$NON-NLS-1$
      logError(Const.getStackTracker(e));
    }    
    
    try {
      URL resolvedJarUrl = null;
      String jarUrlS = environmentSubstitute(jarUrl);
      if (jarUrlS.indexOf("://") == -1) {
        // default to file://
        File jarFile = new File(jarUrlS);
        resolvedJarUrl = jarFile.toURI().toURL();
      } else {
        resolvedJarUrl = new URL(jarUrlS);
      }
      
      final String cmdLineArgsS = environmentSubstitute(cmdLineArgs);

      if (log.isDetailed())
        logDetailed(BaseMessages.getString(PKG, "JobEntryHadoopJobExecutor.ResolvedJar", resolvedJarUrl.toExternalForm()));

      if (isSimple) {
  /*      final AtomicInteger taskCount = new AtomicInteger(0);
        final AtomicInteger successCount = new AtomicInteger(0);
        final AtomicInteger failedCount = new AtomicInteger(0); */
        
        if (log.isDetailed())
          logDetailed(BaseMessages.getString(PKG, "JobEntryHadoopJobExecutor.SimpleMode"));
        List<Class<?>> classesWithMains = JarUtility.getClassesInJarWithMain(resolvedJarUrl.toExternalForm(), getClass().getClassLoader());        
        for (final Class<?> clazz : classesWithMains) {
          Runnable r = new Runnable() {
            public void run() {
              try {
                final ClassLoader cl = Thread.currentThread().getContextClassLoader();
                try {
//                  taskCount.incrementAndGet();
                  Thread.currentThread().setContextClassLoader(clazz.getClassLoader());
                  Method mainMethod = clazz.getMethod("main", new Class[] { String[].class });
                  Object[] args = (cmdLineArgsS != null) ? new Object[] { cmdLineArgsS.split(" ") } : new Object[0];
                  mainMethod.invoke(null, args);
                } finally {
                  Thread.currentThread().setContextClassLoader(cl);
//                  successCount.incrementAndGet();
//                  taskCount.decrementAndGet();
                }
              } catch (Throwable ignored) {
                // skip, try the next one
//                logError(ignored.getMessage());
//                failedCount.incrementAndGet();
                ignored.printStackTrace();
              }
            }
          };
          Thread t = new Thread(r);
          t.start();
        }                

        // uncomment to implement blocking
        /* if (blocking) {
          while (taskCount.get() > 0 && !parentJob.isStopped()) {
            Thread.sleep(1000);
          }

          if (!parentJob.isStopped()) {
            result.setResult(successCount.get() > 0);
            result.setNrErrors((successCount.get() > 0) ? 0 : 1);
          } else {
            // we can't really know at this stage if 
            // the hadoop job will finish successfully 
            // because we have to stop now
            result.setResult(true); // look on the bright side of life :-)...
            result.setNrErrors(0);
          }
        } else { */
          // non-blocking - just set success equal to no failures arising
          // from invocation
//          result.setResult(failedCount.get() == 0);
//          result.setNrErrors(failedCount.get());
        result.setResult(true);
        result.setNrErrors(0);
       /* } */
      } else {
        if (log.isDetailed())
          logDetailed(BaseMessages.getString(PKG, "JobEntryHadoopJobExecutor.AdvancedMode"));

        HadoopShim shim = HadoopConfigurationRegistry.getInstance().getActiveConfiguration().getHadoopShim();
        Configuration conf = shim.createConfiguration();
        FileSystem fs = shim.getFileSystem(conf);
        URL[] urls = new URL[] { resolvedJarUrl };
        URLClassLoader loader = new URLClassLoader(urls, shim.getClass().getClassLoader());
        String hadoopJobNameS = environmentSubstitute(hadoopJobName);
        conf.setJobName(hadoopJobNameS);

        String outputKeyClassS = environmentSubstitute(outputKeyClass);
        conf.setOutputKeyClass(loader.loadClass(outputKeyClassS));
        String outputValueClassS = environmentSubstitute(outputValueClass);
        conf.setOutputValueClass(loader.loadClass(outputValueClassS));

        if(mapperClass != null) {
          String mapperClassS = environmentSubstitute(mapperClass);
          Class<?> mapper = loader.loadClass(mapperClassS);
          conf.setMapperClass(mapper);
        }
        if(combinerClass != null) {
          String combinerClassS = environmentSubstitute(combinerClass);
          Class<?> combiner = loader.loadClass(combinerClassS);
          conf.setCombinerClass(combiner);
        }
        if(reducerClass != null) {
          String reducerClassS = environmentSubstitute(reducerClass);
          Class<?> reducer = loader.loadClass(reducerClassS);
          conf.setReducerClass(reducer);
        }

        if(inputFormatClass != null) {
          String inputFormatClassS = environmentSubstitute(inputFormatClass);
          Class<?> inputFormat = loader.loadClass(inputFormatClassS);
          conf.setInputFormat(inputFormat);
        }
        if(outputFormatClass != null) {
          String outputFormatClassS = environmentSubstitute(outputFormatClass);
          Class<?> outputFormat = loader.loadClass(outputFormatClassS);
          conf.setOutputFormat(outputFormat);
        }

        String hdfsHostnameS = environmentSubstitute(hdfsHostname);
        String hdfsPortS = environmentSubstitute(hdfsPort);
        String jobTrackerHostnameS = environmentSubstitute(jobTrackerHostname);
        String jobTrackerPortS = environmentSubstitute(jobTrackerPort);
        
        List<String> configMessages = new ArrayList<String>();
        shim.configureConnectionInformation(hdfsHostnameS, hdfsPortS, 
            jobTrackerHostnameS, jobTrackerPortS, conf, configMessages);
        for (String m : configMessages) {
          logBasic(m);
        }        

        String inputPathS = environmentSubstitute(inputPath);
        String[] inputPathParts = inputPathS.split(",");
        List<Path> paths = new ArrayList<Path>();
        for (String path : inputPathParts) {
          paths.add(fs.asPath(conf.getDefaultFileSystemURL(), path));
        }
        Path[] finalPaths = paths.toArray(new Path[paths.size()]);
        
        conf.setInputPaths(finalPaths);
        String outputPathS = environmentSubstitute(outputPath);
        conf.setOutputPath(fs.asPath(conf.getDefaultFileSystemURL(), outputPathS));

        // process user defined values
        for (UserDefinedItem item : userDefined) {
          if (item.getName() != null && !"".equals(item.getName()) && item.getValue() != null && !"".equals(item.getValue())) {
            String nameS = environmentSubstitute(item.getName());
            String valueS = environmentSubstitute(item.getValue());
            conf.set(nameS, valueS);
          }
        }

        String workingDirectoryS = environmentSubstitute(workingDirectory);
        conf.setWorkingDirectory(conf.getDefaultFileSystemURL() + workingDirectoryS);
        conf.setJar(jarUrl);

        String numMapTasksS = environmentSubstitute(numMapTasks);
        String numReduceTasksS = environmentSubstitute(numReduceTasks);
        int numM = 1;
        try {
          numM = Integer.parseInt(numMapTasksS);
        } catch (NumberFormatException e) {
          logError("Can't parse number of map tasks '" + numMapTasksS + "'. Setting num" +
          		"map tasks to 1");
        }
        int numR = 1;
        try {
          numR = Integer.parseInt(numReduceTasksS);
        } catch (NumberFormatException e) {
          logError("Can't parse number of reduce tasks '" + numReduceTasksS + "'. Setting num" +
            "reduce tasks to 1");
        }
        
        conf.setNumMapTasks(numM);
        conf.setNumReduceTasks(numR);

        RunningJob runningJob = shim.submitJob(conf);
        
        String loggingIntervalS = environmentSubstitute(loggingInterval);
        int logIntv = 60;
        try {
          logIntv = Integer.parseInt(loggingIntervalS);
        } catch (NumberFormatException e) {
          logError("Can't parse logging interval '" + loggingIntervalS + "'. Setting " +
          "logging interval to 60");
        }
        if (blocking) {
          try {
            int taskCompletionEventIndex = 0;
            while (!parentJob.isStopped() && !runningJob.isComplete()) {
              if (logIntv >= 1) {
                printJobStatus(runningJob);
                taskCompletionEventIndex = logTaskMessages(runningJob, taskCompletionEventIndex);
                Thread.sleep(logIntv * 1000);
              } else {
                Thread.sleep(60000);
              }
            }
            
            if (parentJob.isStopped() && !runningJob.isComplete()) {
              // We must stop the job running on Hadoop
              runningJob.killJob();
              // Indicate this job entry did not complete
              result.setResult(false);
            }
            
            printJobStatus(runningJob);
            // Log any messages we may have missed while polling
            logTaskMessages(runningJob, taskCompletionEventIndex);
          } catch (InterruptedException ie) {
            logError(ie.getMessage(), ie);
          }
          
          // Entry is successful if the MR job is successful overall
          result.setResult(runningJob.isSuccessful());
        }

      }
    } catch (Throwable t) {
      t.printStackTrace();
      result.setStopped(true);
      result.setNrErrors(1);
      result.setResult(false);
      logError(t.getMessage(), t);
    }
    
    if (appender != null)
    {
      LogWriter.getInstance().removeAppender(appender);
      appender.close();
      
      ResultFile resultFile = new ResultFile(ResultFile.FILE_TYPE_LOG, appender.getFile(), parentJob.getJobname(), getName());
      result.getResultFiles().put(resultFile.getFile().toString(), resultFile);
    }
    
    return result;
  }

  /**
   * Log messages indicating completion (success/failure) of component tasks for the provided running job.
   *
   * @param runningJob Running job to poll for completion events
   * @param startIndex Start at this event index to poll from
   * @return Total events consumed
   * @throws IOException Error fetching events
   */
  private int logTaskMessages(RunningJob runningJob, int startIndex) throws IOException {
    TaskCompletionEvent[] tcEvents = runningJob.getTaskCompletionEvents(startIndex);
    for(int i = 0; i < tcEvents.length; i++) {
      String[] diags = runningJob.getTaskDiagnostics(tcEvents[i].getTaskAttemptId());
      StringBuilder diagsOutput = new StringBuilder();

      if(diags != null && diags.length > 0) {
        diagsOutput.append(Const.CR);
        for(String s : diags) {
          diagsOutput.append(s);
          diagsOutput.append(Const.CR);
        }
      }

      switch(tcEvents[i].getTaskStatus()) {
        case KILLED: {
          logError(BaseMessages.getString(PKG, "JobEntryHadoopJobExecutor.TaskDetails", TaskCompletionEvent.Status.KILLED, tcEvents[i].getTaskAttemptId(), tcEvents[i].getTaskAttemptId(), tcEvents[i].getEventId(), diagsOutput)); //$NON-NLS-1$
        }break;
        case FAILED: {
          logError(BaseMessages.getString(PKG, "JobEntryHadoopJobExecutor.TaskDetails", TaskCompletionEvent.Status.FAILED, tcEvents[i].getTaskAttemptId(), tcEvents[i].getTaskAttemptId(), tcEvents[i].getEventId(), diagsOutput)); //$NON-NLS-1$
        }break;
        case SUCCEEDED: {
          logDetailed(BaseMessages.getString(PKG, "JobEntryHadoopJobExecutor.TaskDetails", TaskCompletionEvent.Status.SUCCEEDED, tcEvents[i].getTaskAttemptId(), tcEvents[i].getTaskAttemptId(), tcEvents[i].getEventId(), diagsOutput)); //$NON-NLS-1$
        }break;
      }
    }
    return tcEvents.length;
  }

  public void printJobStatus(RunningJob runningJob) throws IOException {
    if (log.isBasic()) {
      float setupPercent = runningJob.setupProgress() * 100f;
      float mapPercent = runningJob.mapProgress() * 100f;
      float reducePercent = runningJob.reduceProgress() * 100f;
      logBasic(BaseMessages.getString(PKG, "JobEntryHadoopJobExecutor.RunningPercent", setupPercent, mapPercent, reducePercent));
    }
  }

  public void loadXML(Node entrynode, List<DatabaseMeta> databases, List<SlaveServer> slaveServers, Repository rep) throws KettleXMLException {
    super.loadXML(entrynode, databases, slaveServers);
    hadoopJobName = XMLHandler.getTagValue(entrynode, "hadoop_job_name");
    
    isSimple = "Y".equalsIgnoreCase(XMLHandler.getTagValue(entrynode, "simple"));
    jarUrl = XMLHandler.getTagValue(entrynode, "jar_url");
    cmdLineArgs = XMLHandler.getTagValue(entrynode, "command_line_args");
    blocking = "Y".equalsIgnoreCase(XMLHandler.getTagValue(entrynode, "blocking"));
    /*try {
      loggingInterval = Integer.parseInt(XMLHandler.getTagValue(entrynode, "logging_interval"));
    } catch (NumberFormatException nfe) {
    } */
    loggingInterval = XMLHandler.getTagValue(entrynode, "logging_interval");

    mapperClass = XMLHandler.getTagValue(entrynode, "mapper_class");
    combinerClass = XMLHandler.getTagValue(entrynode, "combiner_class");
    reducerClass = XMLHandler.getTagValue(entrynode, "reducer_class");
    inputPath = XMLHandler.getTagValue(entrynode, "input_path");
    inputFormatClass = XMLHandler.getTagValue(entrynode, "input_format_class");
    outputPath = XMLHandler.getTagValue(entrynode, "output_path");
    outputKeyClass = XMLHandler.getTagValue(entrynode, "output_key_class");
    outputValueClass = XMLHandler.getTagValue(entrynode, "output_value_class");
    outputFormatClass = XMLHandler.getTagValue(entrynode, "output_format_class");

    hdfsHostname = XMLHandler.getTagValue(entrynode, "hdfs_hostname");
    hdfsPort = XMLHandler.getTagValue(entrynode, "hdfs_port");
    jobTrackerHostname = XMLHandler.getTagValue(entrynode, "job_tracker_hostname");
    jobTrackerPort = XMLHandler.getTagValue(entrynode, "job_tracker_port");
    //numMapTasks = Integer.parseInt(XMLHandler.getTagValue(entrynode, "num_map_tasks"));
    numMapTasks = XMLHandler.getTagValue(entrynode, "num_map_tasks");
    //numReduceTasks = Integer.parseInt(XMLHandler.getTagValue(entrynode, "num_reduce_tasks"));
    numReduceTasks = XMLHandler.getTagValue(entrynode, "num_reduce_tasks");
    workingDirectory = XMLHandler.getTagValue(entrynode, "working_dir");

    // How many user defined elements?
    userDefined = new ArrayList<UserDefinedItem>();
    Node userDefinedList = XMLHandler.getSubNode(entrynode, "user_defined_list");
    int nrUserDefined = XMLHandler.countNodes(userDefinedList, "user_defined");
    for (int i = 0; i < nrUserDefined; i++) {
      Node userDefinedNode = XMLHandler.getSubNodeByNr(userDefinedList, "user_defined", i);
      String name = XMLHandler.getTagValue(userDefinedNode, "name");
      String value = XMLHandler.getTagValue(userDefinedNode, "value");
      UserDefinedItem item = new UserDefinedItem();
      item.setName(name);
      item.setValue(value);
      userDefined.add(item);
    }
  }

  public String getXML() {
    StringBuffer retval = new StringBuffer(1024);
    retval.append(super.getXML());
    retval.append("      ").append(XMLHandler.addTagValue("hadoop_job_name", hadoopJobName));

    retval.append("      ").append(XMLHandler.addTagValue("simple", isSimple));
    retval.append("      ").append(XMLHandler.addTagValue("jar_url", jarUrl));
    retval.append("      ").append(XMLHandler.addTagValue("command_line_args", cmdLineArgs));
    retval.append("      ").append(XMLHandler.addTagValue("blocking", blocking));
    retval.append("      ").append(XMLHandler.addTagValue("logging_interval", loggingInterval));
    retval.append("      ").append(XMLHandler.addTagValue("hadoop_job_name", hadoopJobName));

    retval.append("      ").append(XMLHandler.addTagValue("mapper_class", mapperClass));
    retval.append("      ").append(XMLHandler.addTagValue("combiner_class", combinerClass));
    retval.append("      ").append(XMLHandler.addTagValue("reducer_class", reducerClass));
    retval.append("      ").append(XMLHandler.addTagValue("input_path", inputPath));
    retval.append("      ").append(XMLHandler.addTagValue("input_format_class", inputFormatClass));
    retval.append("      ").append(XMLHandler.addTagValue("output_path", outputPath));
    retval.append("      ").append(XMLHandler.addTagValue("output_key_class", outputKeyClass));
    retval.append("      ").append(XMLHandler.addTagValue("output_value_class", outputValueClass));
    retval.append("      ").append(XMLHandler.addTagValue("output_format_class", outputFormatClass));

    retval.append("      ").append(XMLHandler.addTagValue("hdfs_hostname", hdfsHostname));
    retval.append("      ").append(XMLHandler.addTagValue("hdfs_port", hdfsPort));
    retval.append("      ").append(XMLHandler.addTagValue("job_tracker_hostname", jobTrackerHostname));
    retval.append("      ").append(XMLHandler.addTagValue("job_tracker_port", jobTrackerPort));
    retval.append("      ").append(XMLHandler.addTagValue("num_map_tasks", numMapTasks));
    retval.append("      ").append(XMLHandler.addTagValue("num_reduce_tasks", numReduceTasks));
    retval.append("      ").append(XMLHandler.addTagValue("working_dir", workingDirectory));

    retval.append("      <user_defined_list>").append(Const.CR);
    if (userDefined != null) {
      for (UserDefinedItem item : userDefined) {
        if (item.getName() != null && !"".equals(item.getName()) && item.getValue() != null && !"".equals(item.getValue())) {
          retval.append("        <user_defined>").append(Const.CR);
          retval.append("          ").append(XMLHandler.addTagValue("name", item.getName()));
          retval.append("          ").append(XMLHandler.addTagValue("value", item.getValue()));
          retval.append("        </user_defined>").append(Const.CR);
        }
      }
    }
    retval.append("      </user_defined_list>").append(Const.CR);
    return retval.toString();
  }

  public void loadRep(Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases, List<SlaveServer> slaveServers) throws KettleException {
    if(rep != null) {
      super.loadRep(rep, id_jobentry, databases, slaveServers);
      
      setHadoopJobName(rep.getJobEntryAttributeString(id_jobentry, "hadoop_job_name"));
      
      setSimple(rep.getJobEntryAttributeBoolean(id_jobentry, "simple"));

      setJarUrl(rep.getJobEntryAttributeString(id_jobentry, "jar_url"));
      setCmdLineArgs(rep.getJobEntryAttributeString(id_jobentry, "command_line_args"));
      setBlocking(rep.getJobEntryAttributeBoolean(id_jobentry, "blocking"));
      //setLoggingInterval(new Long(rep.getJobEntryAttributeInteger(id_jobentry, "logging_interval")).intValue());
      setLoggingInterval(rep.getJobEntryAttributeString(id_jobentry, "logging_interval"));

      setMapperClass(rep.getJobEntryAttributeString(id_jobentry, "mapper_class"));
      setCombinerClass(rep.getJobEntryAttributeString(id_jobentry, "combiner_class"));
      setReducerClass(rep.getJobEntryAttributeString(id_jobentry, "reducer_class"));
      setInputPath(rep.getJobEntryAttributeString(id_jobentry, "input_path"));
      setInputFormatClass(rep.getJobEntryAttributeString(id_jobentry, "input_format_class"));
      setOutputPath(rep.getJobEntryAttributeString(id_jobentry, "output_path"));
      setOutputKeyClass(rep.getJobEntryAttributeString(id_jobentry, "output_key_class"));
      setOutputValueClass(rep.getJobEntryAttributeString(id_jobentry, "output_value_class"));
      setOutputFormatClass(rep.getJobEntryAttributeString(id_jobentry, "output_format_class"));

      setHdfsHostname(rep.getJobEntryAttributeString(id_jobentry, "hdfs_hostname"));
      setHdfsPort(rep.getJobEntryAttributeString(id_jobentry, "hdfs_port"));
      setJobTrackerHostname(rep.getJobEntryAttributeString(id_jobentry, "job_tracker_hostname"));
      setJobTrackerPort(rep.getJobEntryAttributeString(id_jobentry, "job_tracker_port"));
      //setNumMapTasks(new Long(rep.getJobEntryAttributeInteger(id_jobentry, "num_map_tasks")).intValue());
      setNumMapTasks(rep.getJobEntryAttributeString(id_jobentry, "num_map_tasks"));
//      setNumReduceTasks(new Long(rep.getJobEntryAttributeInteger(id_jobentry, "num_reduce_tasks")).intValue());
      setNumReduceTasks(rep.getJobEntryAttributeString(id_jobentry, "num_reduce_tasks"));
      setWorkingDirectory(rep.getJobEntryAttributeString(id_jobentry, "working_dir"));

      
      int argnr = rep.countNrJobEntryAttributes(id_jobentry, "user_defined_name");//$NON-NLS-1$
      if(argnr > 0) {
        userDefined = new ArrayList<UserDefinedItem>();
        
        UserDefinedItem item = null;
        for(int i = 0; i < argnr; i++) {
          item = new UserDefinedItem();
          item.setName(rep.getJobEntryAttributeString(id_jobentry, i,"user_defined_name")); //$NON-NLS-1$
          item.setValue(rep.getJobEntryAttributeString(id_jobentry, i,"user_defined_value")); //$NON-NLS-1$
          userDefined.add(item);
        }
      }
    } else {
      throw new KettleException("Unable to save to a repository. The repository is null."); //$NON-NLS-1$
    }
  }

  public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
    if(rep != null) {
      super.saveRep(rep, id_job);
      
      rep.saveJobEntryAttribute(id_job, getObjectId(),"hadoop_job_name", hadoopJobName); //$NON-NLS-1$
      
      rep.saveJobEntryAttribute(id_job, getObjectId(),"simple", isSimple); //$NON-NLS-1$

      rep.saveJobEntryAttribute(id_job, getObjectId(),"jar_url", jarUrl); //$NON-NLS-1$
      rep.saveJobEntryAttribute(id_job, getObjectId(),"command_line_args", cmdLineArgs); //$NON-NLS-1$
      rep.saveJobEntryAttribute(id_job, getObjectId(),"blocking", blocking); //$NON-NLS-1$
      rep.saveJobEntryAttribute(id_job, getObjectId(),"logging_interval", loggingInterval); //$NON-NLS-1$
      rep.saveJobEntryAttribute(id_job, getObjectId(),"hadoop_job_name", hadoopJobName); //$NON-NLS-1$

      rep.saveJobEntryAttribute(id_job, getObjectId(),"mapper_class", mapperClass); //$NON-NLS-1$
      rep.saveJobEntryAttribute(id_job, getObjectId(),"combiner_class", combinerClass); //$NON-NLS-1$
      rep.saveJobEntryAttribute(id_job, getObjectId(),"reducer_class", reducerClass); //$NON-NLS-1$
      rep.saveJobEntryAttribute(id_job, getObjectId(),"input_path", inputPath); //$NON-NLS-1$
      rep.saveJobEntryAttribute(id_job, getObjectId(),"input_format_class", inputFormatClass); //$NON-NLS-1$
      rep.saveJobEntryAttribute(id_job, getObjectId(),"output_path", outputPath); //$NON-NLS-1$
      rep.saveJobEntryAttribute(id_job, getObjectId(),"output_key_class", outputKeyClass); //$NON-NLS-1$
      rep.saveJobEntryAttribute(id_job, getObjectId(),"output_value_class", outputValueClass); //$NON-NLS-1$
      rep.saveJobEntryAttribute(id_job, getObjectId(),"output_format_class", outputFormatClass); //$NON-NLS-1$

      rep.saveJobEntryAttribute(id_job, getObjectId(),"hdfs_hostname", hdfsHostname); //$NON-NLS-1$
      rep.saveJobEntryAttribute(id_job, getObjectId(),"hdfs_port", hdfsPort); //$NON-NLS-1$
      rep.saveJobEntryAttribute(id_job, getObjectId(),"job_tracker_hostname", jobTrackerHostname); //$NON-NLS-1$
      rep.saveJobEntryAttribute(id_job, getObjectId(),"job_tracker_port", jobTrackerPort); //$NON-NLS-1$
      rep.saveJobEntryAttribute(id_job, getObjectId(),"num_map_tasks", numMapTasks); //$NON-NLS-1$
      rep.saveJobEntryAttribute(id_job, getObjectId(),"num_reduce_tasks", numReduceTasks); //$NON-NLS-1$
      rep.saveJobEntryAttribute(id_job, getObjectId(),"working_dir", workingDirectory); //$NON-NLS-1$

      if (userDefined != null) {
        for (int i = 0; i < userDefined.size(); i++) {
          UserDefinedItem item = userDefined.get(i);
          if (item.getName() != null && !"".equals(item.getName()) && item.getValue() != null && !"".equals(item.getValue())) { //$NON-NLS-1$ //$NON-NLS-2$
            rep.saveJobEntryAttribute(id_job, getObjectId(), i,"user_defined_name", item.getName()); //$NON-NLS-1$
            rep.saveJobEntryAttribute(id_job, getObjectId(), i,"user_defined_value", item.getValue()); //$NON-NLS-1$
          }
        }
      }
      
    } else {
      throw new KettleException("Unable to save to a repository. The repository is null."); //$NON-NLS-1$
    }
  }
  
  public boolean evaluates()
  {
    return true;
  }

  public boolean isUnconditional()
  {
    return true;
  }

}
