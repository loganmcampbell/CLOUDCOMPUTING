
import java.io.IOException;
//additonal libs
import java.util.*;
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
public class BigramCount
{

  public static int test = 0;
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
  {

        // IntWritable is just like an integer.
        private final static IntWritable one = new IntWritable(1);

        // TEXT is just like a String in Java, but Hadoop uses this because of the class having
        // COMPARABLE, WRITABLE, AND WRITABLE COMPARABLE

        // COMPARABLE : USED FOR COMPARING WHEN THE REDUCER SORTS THE KEYS
        // WRITABLE : CAN WRITE THE RESULTS TO THE LOCAL DISK
        //NEW WORD (KEY)


        private String mcount = "Cloud Computing Map Counts : ";
        private String bcount = "Cloud Computing Bigram Counts : ";


        private Text mapcount = new Text(mcount);
        private Text bigram = new Text(bcount);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
                      //BREAKS DOWN A STRING INTO TOKENS
                      // CONTAIN THE ARRAY OF WORDS
                      StringTokenizer itr = new StringTokenizer(value.toString());
                      ArrayList<String> bigrams = new ArrayList<String>();
                      boolean skip = false;

                      if (itr.countTokens() <= 1)
                      {
                        skip = true;
                      }
                      else
                      {

                        skip = false;
                      }


                      if (skip == false)
                      {

                        String before = "";
                        String now = "";
                        String after = "";
                        // IF THERE ARE MORE TOKENS THEN ONE
                        while (itr.hasMoreTokens())
                        {
                          if (before.isEmpty())
                        {
                          before = itr.nextToken();

                        }
                        now = itr.nextToken();
                        after = before + " " + now;
                        bigrams.add(after);
                        before = now;
                        now = "";
                        context.write(bigram, one);
                        }
                        context.write(mapcount,one);
                        skip = true;
                    }
        }

  }

  //Method is called once for each key! Reduces it from one object to a <key, frequency>
  //Gets the number of times the key has been hit or has been reached again...hence ^ (frequency)
  //----------------------------------------------------------------------------------------------
  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>
  {
    private IntWritable result = new IntWritable();
    private static int mapslaunched = 0;

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
      int sum = 0;
      for (IntWritable val : values)
      {
        sum += val.get();

      }

      result.set(sum);
      context.write(key, result);
      mapslaunched++;
      System.out.println("NUMBER OF MAPS : " + mapslaunched);
    }

  }

  public static void main(String[] args) throws Exception
  {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2)
    {
      System.err.println("Usage: bigramcount <in> <out>");
      System.exit(2);
    }

    Job job = new Job(conf, "bigram count");

    job.setJarByClass(BigramCount.class);
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
