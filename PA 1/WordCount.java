
import java.io.IOException;
//additonal libs
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.*;
//
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class WordCount
{

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
  {
    public static int counter = 0;
    public static int counter2 = 0;

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
          counter2++;
          PrintWriter out1 = new PrintWriter( new FileWriter("words.txt",false));
          PrintWriter out2 = new PrintWriter( new FileWriter("map.txt",false));

          StringTokenizer itr = new StringTokenizer(value.toString());
          while (itr.hasMoreTokens())
          {
            ++counter;
            word.set(itr.nextToken());
            context.write(word, one);
          }
          out1.print(counter);
          out1.close();
          out2.print(counter2);
          out2.close();
          System.out.println("WORDS : " + counter);
          System.out.println("MAP : " + counter2);

        }

  }

  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>
  {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
      int sum = 0;
      for (IntWritable val : values)
      {
        sum += val.get();
      }

      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception
  {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2)
    {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }

    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
