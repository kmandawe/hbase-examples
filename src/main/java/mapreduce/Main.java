package mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

public class Main {

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();
    Job job = Job.getInstance(conf);
    job.setJarByClass(Main.class);

    String sourceTale = "census";
    String targetTable = "summary";

    Scan scan = new Scan();
    scan.setCaching(500);
    scan.setCacheBlocks(false);

    TableMapReduceUtil.addDependencyJars(job);

    TableMapReduceUtil.initTableMapperJob(
        sourceTale,
        scan,
        MaritalStatusMapper.class,
        ImmutableBytesWritable.class,
        IntWritable.class,
        job);

    TableMapReduceUtil.initTableReducerJob(targetTable, MaritalStatusReducer.class, job);

    job.setNumReduceTasks(1);
    job.waitForCompletion(true);
  }
}
