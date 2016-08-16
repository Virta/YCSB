package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;

import java.util.Properties;

/**
 * Created by frojala on 16/08/16.
 */
public class GeodeWorkload extends Workload {

  @Override
  public void init(Properties properties) throws WorkloadException {

  }

  @Override
  public Object initThread(Properties properties, int myThreadId, int threadCount) throws WorkloadException {
    return null;
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    return false;
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    return false;
  }

  @Override
  public void cleanup() throws WorkloadException {

  }

}
