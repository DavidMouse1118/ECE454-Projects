import java.io.IOException;
import java.util.HashMap;
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
import org.apache.hadoop.filecache.DistributedCache;
import java.net.URI;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.*;  

// Performance: 11.59user 0.92system 0:49.22elapsed 25%CPU (0avgtext+0avgdata 401988maxresident)k
public class Task4 {

  public static class MovieSimilarityMapper extends Mapper<Object, Text, Text, NullWritable> {
    private Text result = new Text();
    private HashMap<String, Byte[]> movieRatingsMap = new HashMap<String, Byte[]>();

    public void setup(Context context) throws IOException, InterruptedException {
      Path cachePath = context.getLocalCacheFiles()[0];
      BufferedReader reader = new BufferedReader(new FileReader(cachePath.toString())); 
      String line;
      
      while ((line = reader.readLine()) != null){
        String[] tokens = line.split(",", -1);
        String title = tokens[0];
        Byte[] ratings = new Byte[tokens.length - 1];

        for (int i = 0; i < ratings.length; i++) {
          if (!tokens[i + 1].isEmpty()) {
            ratings[i] = Byte.parseByte(tokens[i + 1]);
          } else {
            ratings[i] = 0;
          }
        }

        movieRatingsMap.put(title, ratings);
      }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String title1 = value.toString().split(",", 2)[0];
      Byte[] ratings1 = movieRatingsMap.get(title1);

      for (String title2 : movieRatingsMap.keySet()) {
        if (title2.compareTo(title1) > 0) {
          Byte[] ratings2 = movieRatingsMap.get(title2);
          int similarity = findSimilarity(ratings1, ratings2);

          result.set(title1 + "," + title2 + "," + similarity);
          context.write(result, NullWritable.get());
        }
      }
    }

    public int findSimilarity(Byte[] ratings1, Byte[] ratings2) {
      int similarity = 0;

      for (int i = 0; i < Math.min(ratings1.length, ratings2.length); i++) {
        if (ratings1[i] == ratings2[i] && ratings1[i] != 0) {
          similarity ++;
        }
      }

      return similarity;
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    Job job = new Job(conf, "Task4");
    job.setJarByClass(Task4.class);

    job.setMapperClass(MovieSimilarityMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    // Set up distributed cache
    job.addCacheFile(new URI(otherArgs[0]));
    // This is a mapper-only hadoop job
    job.setNumReduceTasks(0);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
