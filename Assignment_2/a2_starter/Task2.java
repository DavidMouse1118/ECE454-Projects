import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task2 {

  public static class RatingCountMapper extends Mapper<Object, Text, NullWritable, IntWritable> {
    private Text Counter = new Text("Counter");
    private IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",", -1);
    
      for (int i = 1; i < tokens.length; i++) {
        if (tokens[i].equals("")) {
          continue;
        }

        context.write(NullWritable.get(), one);
      }
    }
  }

  public static class RatingCountReducer extends Reducer <NullWritable, IntWritable, NullWritable, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(NullWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int totalCount = 0;

      for (IntWritable val : values) {
        totalCount += val.get();
      }

      result.set(totalCount);
      context.write(NullWritable.get(), result);
    }
  }
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    Job job = new Job(conf, "Task2");
    job.setJarByClass(Task2.class);

    job.setMapperClass(RatingCountMapper.class);
    job.setCombinerClass(RatingCountReducer.class);
    job.setReducerClass(RatingCountReducer.class);

    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(IntWritable.class);

    // force the framework to use a single reducer.
    job.setNumReduceTasks(1);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
