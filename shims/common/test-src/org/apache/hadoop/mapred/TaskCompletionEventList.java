package org.apache.hadoop.mapred;

import java.util.ArrayList;

import org.apache.hadoop.mapred.TaskCompletionEvent;

/**
 * This is an empty, fake class necessary for all non-MapR shims because MapR's Hadoop
 * adds a method to the RunningJob interface that returns a class that only exists in
 * the MapR Hadoop distribution. So we add a fake one here so the common tests will 
 * compile and run successfully.
 */
public class TaskCompletionEventList extends ArrayList<TaskCompletionEvent> {
  
}
