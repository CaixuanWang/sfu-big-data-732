import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;  
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WikipediaPopular extends Configured implements Tool{

  public static class WikipediaPopularMapper
       extends Mapper<LongWritable, Text, Text, LongWritable>{
        private Text filetext = new Text();
        LongWritable countname = new LongWritable();
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = value.toString();
      String[] variable = line.split("\\s+");
      String file = variable[0];
      String place = variable[1];
      String title = variable[2];
      String count = variable[3];
      if (place.equals("en")){
        if(!title.equals("Main_Page") && !title.startsWith("Special:")){
          countname.set(Long.parseLong(count));
          filetext.set(file);
        }
        context.write(filetext,countname);
      }
      
    }
  }

  public static class WikipediaPopularReducer
       extends Reducer<Text,LongWritable,Text,LongWritable> {
    // private DoubleWritable result = new DoubleWritable();
        LongWritable maxlong = new LongWritable();
    public void reduce(Text key, Iterable<LongWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      long max = 0;
      // long filename;
      for (LongWritable val : values) {
        if(max < val.get()){
          // filename = val.get_0();
          max = val.get();
        }
      }
      maxlong.set(max);
      context.write(key, maxlong);
    }
  }
  // public static class RedditAverageCombiner
  //      extends Reducer<Text,LongPairWritable,Text,LongPairWritable> {
  //   // private DoubleWritable result = new DoubleWritable();

  //   public void combine(Text key, Iterable<LongPairWritable> values,
  //                      Context context
  //                      ) throws IOException, InterruptedException {
  //     int all_comment = 0;
  //     int all_score = 0;
  //     for (LongPairWritable val : values) {
  //       all_comment += val.get_0();
  //       all_score += val.get_1();
  //     }
  //     LongPairWritable final0 = new LongPairWritable(all_comment,all_score);
  //     // result.set((double)all_score/(double)all_comment);
  //     context.write(key, final0);
  //   }
  // }
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();
    Job job = Job.getInstance(conf, "WikipediaPopular");
    job.setJarByClass(WikipediaPopular.class);

    job.setInputFormatClass(TextInputFormat.class);

    job.setMapperClass(WikipediaPopularMapper.class);
    //job.setCombinerClass(RedditAverageCombiner.class);
    job.setReducerClass(WikipediaPopularReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextInputFormat.addInputPath(job, new Path(args[0]));
    TextOutputFormat.setOutputPath(job, new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }
  // public static void main(String[] args) throws Exception {
  //   Configuration conf = new Configuration();
  //   Job job = Job.getInstance(conf, "word count");
  //   job.setJarByClass(RedditAverage.class);
  //   job.setMapperClass(RedditAverageMapper.class);
  //   job.setCombinerClass(RedditAverageReducer.class);
  //   job.setReducerClass(RedditAverageReducer.class);
  //   job.setOutputKeyClass(Text.class);
  //   job.setOutputValueClass(LongWritable.class);
  //   FileInputFormat.addInputPath(job, new Path(args[0]));
  //   FileOutputFormat.setOutputPath(job, new Path(args[1]));
  //   System.exit(job.waitForCompletion(true) ? 0 : 1);
  // }
}