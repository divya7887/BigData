import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class Question1 {
public static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {
private static IntWritable one = new IntWritable(1);
private Text users = new Text();
@Override
public void setup(Context context) {
Configuration conf = context.getConfiguration();

}
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
// read the lines from users.dat
String line = value.toString();
// split using :: as the delimiter
String tokens[] = line.split("\\::");
users.set(tokens[0]); 
if(((tokens[1]).equalsIgnoreCase("M"))&& Integer.parseInt(tokens[2]) == 7) { 
context.write(users, NullWritable.get());
}
}
}


public static class Reduce extends Reducer<Text, NullWritable, Text, NullWritable> {
private final static IntWritable userID = new IntWritable();
public void reduce(Text key, NullWritable values, Context context) throws IOException, InterruptedException {
context.write(key, NullWritable.get());
}
}
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job = new Job(conf, "question1");
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(NullWritable.class);
job.setJarByClass(Question1.class);
job.setMapperClass(Map.class);
job.setCombinerClass(Reduce.class);
job.setReducerClass(Reduce.class);
job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.waitForCompletion(true);
}
}
