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


package org.pentaho.hadoop.sample.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount2 extends Configured implements Tool {
  public int run( String[] strings ) throws Exception {
    Configuration conf = getConf();

    Job job = Job.getInstance( conf, "wordcount2" );
    job.setJarByClass( WordCount2.class );

    job.setOutputKeyClass( Text.class );
    job.setOutputValueClass( IntWritable.class );

    job.setMapperClass( Map.class );
    job.setReducerClass( Reduce.class );

    job.setInputFormatClass( TextInputFormat.class );
    job.setOutputFormatClass( TextOutputFormat.class );

    FileInputFormat.addInputPath( job, new Path( strings[ 0 ] ) );
    FileOutputFormat.setOutputPath( job, new Path( strings[ 1 ] ) );

    return job.waitForCompletion( true ) ? 0 : 1;
  }

  public static class Map
    extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable one = new IntWritable( 1 );
    private Text word = new Text();

    public void map( LongWritable key, Text value, Context context )
      throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer( line );
      while ( tokenizer.hasMoreTokens() ) {
        this.word.set( tokenizer.nextToken() );
        context.write( this.word, one );
      }
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce( Text key, Iterable<IntWritable> values, Context context )
      throws IOException, InterruptedException {
      int sum = 0;
      for ( IntWritable val : values ) {
        sum += val.get();
      }
      context.write( key, new IntWritable( sum ) );
    }
  }

  public static void main( String[] args ) throws Exception {
    int exitCode = ToolRunner.run( new Configuration(), new WordCount2(), args );
    System.exit( exitCode );
  }
}
