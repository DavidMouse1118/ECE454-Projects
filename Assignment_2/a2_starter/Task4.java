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
import org.apache.hadoop.filecache.DistributedCache;
import java.net.URI;
import org.apache.hadoop.fs.FileSystem; 
import java.io.*;  

// Performance: 14.48user 2.08system 13:05.92elapsed 2%CPU (0avgtext+0avgdata 410672maxresident)k
public class Task4 {

  public static class MovieSimilarityMapper extends Mapper<Object, Text, Text, NullWritable> {
    private Text result = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      URI[] cacheFiles = context.getCacheFiles();

      if (cacheFiles != null && cacheFiles.length > 0) {
        try {
          String line = "";
          FileSystem fs = FileSystem.get(context.getConfiguration());
          Path getFilePath = new Path(cacheFiles[0].toString());

          BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

          while ((line = reader.readLine()) != null) {
            if (value.toString().compareTo(line) < 0) {
              String similarity = findMovieSimilarity(value.toString(), line);

              result.set(similarity);
              context.write(result, NullWritable.get());
            }
          }
        }

        catch (Exception e) {
          System.out.println("Unable to read the File");
        }
      }
    }

    public String findMovieSimilarity(String movie1, String movie2) {
      String[] ratings1 = movie1.split(",");
      String[] ratings2 = movie2.split(",");

      String moviePair = ratings1[0] + "," + ratings2[0];
      int similarity = 0;

      for (int i = 1; i < Math.min(ratings1.length, ratings2.length); i++) {
        if (ratings1[i].equals(ratings2[i]) == true && ratings1[i].equals("") == false) {
          similarity ++;
        }
      }

      return moviePair + ',' + similarity;
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
