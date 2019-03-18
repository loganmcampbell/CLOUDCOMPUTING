import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.HashMap;

public class InvertedIndex
{

    public static class InvertedIndexMap extends Mapper<LongWritable, Text, WordPair, IntWritable>
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            HashMap<WordPair, IntWritable> map = new HashMap<WordPair, IntWritable>();
            /*Get the name of the file using context.getInputSplit()method*/
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            String line = value.toString();
            //Split the line in words
            String words[] = line.split(" ");
            for (String s : words)
             {
                WordPair WordPair = new WordPair(new Text(s), new Text(fileName));
                if (map.containsKey(WordPair))
                {
                    map.put(WordPair, new IntWritable(map.get(WordPair).get() + 1));
                }
                else
                {
                    map.put(WordPair, new IntWritable(1));
                }
            }
            for (HashMap.Entry<WordPair, IntWritable> entry : map.entrySet())
            {
                WordPair contain = entry.getKey();
                IntWritable num = entry.getValue();
                context.write(contain, num);
            }
        }
    }


    public static class InvertedIndexIntSumReducer extends Reducer<WordPair, IntWritable, Text, Text>
    {
        public StringBuilder tokenstring = new StringBuilder();
        public Text wordindicator = new Text();

        @Override
        public void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {

            if (check(key))
            {
                context.write(wordindicator, new Text(tokenstring.toString()));
                tokenstring = new StringBuilder();
            }
            int count = 0;
            for(IntWritable value : values)
            {
                count += value.get();
            }
            tokenstring.append(key.getNeighbor().toString());
            tokenstring.append(" := ");
            tokenstring.append(count);
            tokenstring.append(";");
            wordindicator.set(key.getWord());
        }

        public boolean check(WordPair key)
        {
            return !key.getWord().equals(wordindicator) && !wordindicator.equals("");
        }


        public void close(WordPair key, Context context) throws IOException, InterruptedException
        {
            context.write(wordindicator, new Text(tokenstring.toString()));
        }
    }


    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2)
        {
            System.err.println("Usage: wordcount [inverted index] <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Inverted Indext");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMap.class);
        job.setMapOutputKeyClass(WordPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(InvertedIndexIntSumReducer.class);
        job.setNumReduceTasks(3);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
