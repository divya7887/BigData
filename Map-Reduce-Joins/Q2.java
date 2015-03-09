import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.mapreduce.*;


import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 *
 * @author Divya
 */
public class Q2 {

   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
//input ratings.dat
private HashMap<Integer, String> userDetails = new HashMap<Integer, String>();
int movieID;
private Text valKey = new Text();
@Override
public void setup(Context context) throws IOException, InterruptedException {
Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
Configuration conf = context.getConfiguration();
movieID = Integer.parseInt(conf.get("movieID"));
File myFile = new File(files[0].getName());
BufferedReader br = new BufferedReader(new FileReader(myFile));
String str = null;
while ((str = br.readLine()) != null) {
String[] tokens = str.split("\\::");
if (Integer.parseInt(tokens[2])>= 4 && Integer.parseInt(tokens[1]) == movieID) {
String value = "";
userDetails.put(Integer.parseInt(tokens[0].trim()), value);
}
}
br.close();
}
   
   
   
   
   //input users.dat
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
String line = value.toString();
Text genderAge = new Text();
String[] tokens = line.split("\\::");
int userID = Integer.parseInt(tokens[0]);
valKey.set(tokens[0]);
genderAge.set(tokens[1]+" "+tokens[2]);
if (userDetails.containsKey(userID)) {
context.write(valKey, genderAge); 
}
}
}
   
public static class Reduce extends Reducer<Text, IntWritable, Text, NullWritable> {
private Text output = new Text();
public void reduce(Text key, NullWritable values, Context context) throws IOException, InterruptedException {
context.write(key, NullWritable.get());
}
   }

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException, ClassNotFoundException {
Configuration conf = new Configuration();
DistributedCache.addCacheFile(new URI(args[0]), conf);
conf.set("movieID", args[2]);
Job job = new Job(conf, "Q2");
job.setJarByClass(Q2.class);
job.setMapperClass(Map.class);
job.setReducerClass(Reduce.class);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(Text.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(NullWritable.class);
FileInputFormat.addInputPath(job, new Path(args[1]));
FileOutputFormat.setOutputPath(job, new Path(args[3]));
job.waitForCompletion(true);
}
}
