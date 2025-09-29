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


package org.pentaho.hadoop.sample.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class WordCount {
  public static void main(String[] args) throws Exception {
    String hdfsHost = "localhost:9000";
    String jobTrackerHost = "localhost:9001";
    String fsPrefix = "hdfs";
    
    String dirInput = "/wordcount/input";
    String dirOutput = "/wordcount/output";
    
    if (args.length == 1 && (
          args[0].equals("--help") ||
          args[0].equals("-h") ||
          args[0].equals("/?")
        )) {
      System.out.println("Usage: WordCount <options>");
      System.out.println();
      System.out.println("Options:");
      System.out.println();
      System.out.println("--input=DIR                   The directory containing the input files for the");
      System.out.println("                              WordCount Hadoop job");
      System.out.println("--output=DIR                  The directory where the results of the WordCount");
      System.out.println("                              Hadoop job will be stored");
      System.out.println("--hdfsHost=HOST               The host<:port> of the HDFS service");
      System.out.println("                              e.g.- localhost:9000");
      System.out.println("--jobTrackerHost=HOST         The host<:port> of the job tracker service");
      System.out.println("                              e.g.- localhost:9001");
      System.out.println("--fsPrefix=PREFIX             The prefix to use for for the filesystem");
      System.out.println("                              e.g.- hdfs");
      System.out.println();
      System.out.println();
      System.out.println("If an option is not provided through the command prompt the following defaults");
      System.out.println("will be used:");
      System.out.println("--input='/wordcount/input'");
      System.out.println("--output='/wordcount/output'");
      System.out.println("--hdfsHost=localhost:9000");
      System.out.println("--jobTrackerHost=localhost:9001");
      System.out.println("--fsPrefix=hdfs");
          
    } else {
      if(args.length > 0){
        for(String arg : args) {
          if(arg.startsWith("--input=")) {
            dirInput = WordCount.getArgValue(arg);
          } else if(arg.startsWith("--output=")) {
            dirOutput = WordCount.getArgValue(arg);
          } else if(arg.startsWith("--hdfsHost=")) {
            hdfsHost = WordCount.getArgValue(arg);
          } else if(arg.startsWith("--jobTrackerHost=")) {
            jobTrackerHost = WordCount.getArgValue(arg);
          } else if(arg.startsWith("--fsPrefix=")) {
            fsPrefix = WordCount.getArgValue(arg);
          }
        }
      }
      
      JobConf conf = new JobConf(WordCount.class);
      conf.setJobName("WordCount");

      String hdfsBaseUrl = fsPrefix + "://" + hdfsHost;
      conf.set("fs.default.name", hdfsBaseUrl + "/");
      if (jobTrackerHost != null && jobTrackerHost.length() > 0) {
        conf.set("mapred.job.tracker", jobTrackerHost);
      }
      
      FileInputFormat.setInputPaths(conf, new Path[] { new Path(hdfsBaseUrl + dirInput) });
      FileOutputFormat.setOutputPath(conf, new Path(hdfsBaseUrl + dirOutput));

      conf.setMapperClass(WordCountMapper.class);
      conf.setReducerClass(WordCountReducer.class);

      conf.setMapOutputKeyClass(Text.class);
      conf.setMapOutputValueClass(IntWritable.class);

      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(IntWritable.class);

      JobClient.runJob(conf);
    }
  }
  
  private static String getArgValue(String arg) {
    String result = null; 
    
    String[] tokens = arg.split("="); 
    if(tokens.length > 1) {
      result = tokens[1].replace("'", "").replace("\"", "");
    }
    System.out.println(arg + " parses to " + result);    
    return result;
  }
}
