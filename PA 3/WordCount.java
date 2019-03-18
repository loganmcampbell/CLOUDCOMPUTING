
import java.io.IOException;
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

//MapReduce is a programming model for processing large data sets with a parallel, distributed algorithm on a cluster"
public class WordCount
{
	//maps input key/value pairs to a set of intermediate key/value pairs
	//The output of the mapper will be the input of the reducers

	/*
	Mapper uses a class that extends Mapper and specifies Object and Text
	as the classes of key/value pairs for the output to the reducers
	*/

	//====================
	//DataType--HadoopType
	//Integer--IntWritable
	//Double--LongWritable
	//String--TextWritable
	//Map--MapWritable
	//Array--Writable


  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
  {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

	//map() takes key/value pair as argument and hadoop context
	//each time its called by hadoop  recieves an offset of the file where the value is as the key and line of the text file we are reading
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		/*
		StringTokenizer to split the line into every single word;
		then it sets the word in the Text object and maps it the the value of 1;
		then writes it to the mappers via the Hadoop context.
		*/
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens())
	  {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  //REDUCER
  //Takes mapper output and uses it as input - REDUCER
  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>
  {
    private IntWritable result = new IntWritable();

	//The reduce method now has to sum all the occurrences of every single word
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


  //EXECUTE JAR FILES
  public static void main(String[] args) throws Exception
  {
	//SETUP CONFIG OBJECT
    Configuration conf = new Configuration();

	//CHECKS THE NUMBER OF ARGUMENTS ARE CORRECT
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2)
	{
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }

	//CREATES JOB OBJECT
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class); //sets the Jar by finding where a given class came from
    job.setMapperClass(TokenizerMapper.class);//sets the class that will be executed as the mapper
    job.setCombinerClass(IntSumReducer.class);//sets the class that will be executed as the combiner
    job.setReducerClass(IntSumReducer.class);//sets the class that will be executed as the reducer
    job.setOutputKeyClass(Text.class);//sets the class that will be used as the key for outputting data to the user
    job.setOutputValueClass(IntWritable.class);//sets the class that will be used as the value for outputting data to the user
    job.setSortComparatorClass(NewText.class);// sets the class that will sort before going to the reducer.
	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//TELL HADOOP TO FIND THE INPUT WITH PATHS
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    System.exit(job.waitForCompletion(true) ? 0 : 1);// SUBMITS THE JOB TO THE CLUSTER AND WAITS FOR IT TO FINISH.
  }
}
