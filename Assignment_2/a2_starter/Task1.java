import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task1 {

  public static class MovieRatingSplit extends Mapper<Object, Text, Text, Text> {
    private Text movieTitle  = new Text();
    private Text result = new Text();

    public void map(Object Key, Text value, Context context) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",");
      movieTitle.set(tokens[0]);

      List<String> maxIndex = getMaxIndex(Arrays.copyOfRange(tokens, 1, tokens.length));

      result.set(String.join(",", maxIndex));
      context.write(movieTitle, result);
    }

    public List<String> getMaxIndex(String[] movieRating) {
      List<String> maxIndex = new ArrayList<>();
      int max = Integer.MIN_VALUE;

      for (int i = 0; i < movieRating.length; i++) {
        if (movieRating[i].equals("")) {
          continue;
        }

        int rating = Integer.parseInt(movieRating[i]);

        if (rating == max) {
          maxIndex.add(String.valueOf(i + 1));
        } else if (rating > max){
          maxIndex.clear();
          maxIndex.add(String.valueOf(i + 1));
          max = rating;
        }
      }

      return maxIndex;
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    Job job = new Job(conf, "Task1");
    job.setJarByClass(Task1.class);

    job.setMapperClass(MovieRatingSplit.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // This is a mapper-only hadoop job
    job.setNumReduceTasks(0);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
