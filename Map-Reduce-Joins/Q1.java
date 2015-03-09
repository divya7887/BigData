import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.*;
public class Q11 {

   // getting Female userId from users list
public static class Map0 extends Mapper<LongWritable, Text, Text, Text> {
private Text userId = new Text();
private Text rating = new Text();
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
String line = value.toString();
String[] tokens = line.split("\\::");
String userid = tokens[0];

if((tokens[1]).equalsIgnoreCase("F")){
    userId.set(userid);
    rating.set("$$");
context.write(userId, rating); 
}
}   
}    
    
// getting userId and ratings from ratings list
public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
private Text userId = new Text();
private Text rating = new Text();
//private static IntWritable rating = new IntWritable();
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
String line = value.toString();
String[] tokens = line.split("\\::");
userId.set(tokens[0]);
rating.set("##" + tokens[1]+"::"+tokens[2]);
context.write(userId, rating); 
}
}
    
 // reducer performs reduce side join and outputs ratings by females for each movie 
public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
private Text tmp = new Text();
private Text movierating = new Text();
private Text userID = new Text();
private Text movieId = new Text();
private ArrayList<Text> listUser = new ArrayList<Text>();
private ArrayList<Text> listRating = new ArrayList<Text>();
@Override
public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    listUser.clear();
			listRating.clear();
			String[] rating = null;
Iterator<Text> itr = values.iterator();
boolean first = false;
boolean second = false;
while (itr.hasNext()) {
tmp = itr.next();
if (tmp.toString().contains("##")) {
    listRating.add(new Text(tmp.toString().substring(2)));
}
else if (tmp.toString().contains("$$")) {
    listUser.add(new Text(tmp.toString().substring(2)));
}
}
if (!listUser.isEmpty() && !listRating.isEmpty()) {
				for (Text userRecords : listUser) {
					for (Text ratingRecords : listRating) {
						try {
							rating = ratingRecords.toString().split("::");
							String movieID = rating[0].trim();
							String rat = rating[1].trim();
                          
							movieId.set((movieID));
							movierating.set(rat);
							context.write(movieId, movierating);
						} catch (Exception e) {
                         
						}
					}
				}
			}
}
}

//to find the average of movie rated by females

//  mapper to process first mapreduce output
public static class Map2 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
private static DoubleWritable ratings = new DoubleWritable();
private Text movieID = new Text();
private Text userID = new Text();
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
String line = value.toString();
String[] tokens = line.split("\\s");
movieID.set(tokens[0]);
 ratings.set(Double.parseDouble(tokens[1]));
context.write(movieID,ratings);
}
}


public static class Reduce2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
private static DoubleWritable avgRating = new DoubleWritable();
//@Override
public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
throws IOException, InterruptedException {
double sum = 0.0;
double count = 0.0;

for (DoubleWritable val : values) {
sum += val.get();
count++;
}
double avg = sum/count;
avgRating.set(avg);
context.write(key, avgRating); // outputs movie,avgRating as input for second mapper
}
}

// sort and produce top 5 ratings
public static class Map3 extends Mapper<LongWritable, Text, NullWritable, Text> {
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
context.write(NullWritable.get(), value);
}
}
//to sort by value
public static class ValueComparator implements Comparator {
Map map;
public ValueComparator(Map map) {
this.map = map;
}
public int compare(Object keyA, Object keyB) {
Double valueA = (Double) map.get(keyA);
Double valueB = (Double) map.get(keyB);
if (valueA == valueB || valueB > valueA) {
return 1;
} else {
return -1;
}
}
}



public static class Reduce3 extends Reducer<NullWritable, Text, Text, DoubleWritable> {
private static Map<String, Double> movies = new HashMap();
private Text word = new Text();
private static DoubleWritable avg = new DoubleWritable();
@Override
public void reduce(NullWritable key, Iterable<Text> values, Context context)
throws IOException, InterruptedException {
for (Text value : values) {
String[] tokens = value.toString().split("[\\s]+");
String movie = tokens[0];
Double avgRating = Double.parseDouble(tokens[1]);
movies.put(movie, avgRating);
}
Map<String, Double> treeMapsort = new TreeMap(new ValueComparator(movies));
//treemap for sorting
treeMapsort .putAll(movies);
int count = 0;
for (Map.Entry<String, Double> entry : treeMapsort .entrySet()) {
count++;
word.set(entry.getKey());
avg.set(entry.getValue());
context.write(word, avg);
if (count == 5) {
break;
}
}
}
}//out3



// mapper to process movies.dat file for movie name
public static class Map4 extends Mapper<LongWritable, Text, Text, Text> {
private Text title = new Text();
private Text movieID = new Text();
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
String line = value.toString();
String[] tokens = line.split("\\::");
title.set("##" + tokens[1]);
movieID.set(tokens[0]);
context.write(movieID, title);
}
}
//mapper processing output from temporary file
public static class Map5 extends Mapper<LongWritable, Text, Text, Text> {
private Text title = new Text();
private Text movieID = new Text();
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
String line = value.toString();
String[] tokens = line.split("\\t");
title.set("$$"+tokens[1]);
movieID.set(tokens[0]);
context.write(movieID, title);
}
}
// reducer performs reduce side join 
public static class Reduce4 extends Reducer<Text, Text, Text, Text> {
private Text tmp = new Text();
private Text title = new Text();
private Text avgrating = new Text();
@Override
public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
Iterator<Text> itr = values.iterator();
boolean first = false;
boolean second = false;
while (itr.hasNext()) {
tmp = itr.next();
if (tmp.toString().contains("##")) {
first=true;
title.set(tmp.toString().substring(2));
} if (tmp.toString().contains("$$")) {
second=true;
avgrating.set(tmp.toString().substring(2));
}
}
if(first && second)
context.write(title, avgrating);
}
}



    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
       boolean waitMapperReduce1 = false;
Configuration conf1 = new Configuration();
Job job1 = new Job(conf1, "job1");
MultipleInputs.addInputPath(job1, new Path(args[0]),
TextInputFormat.class, Map0.class);
MultipleInputs.addInputPath(job1, new Path(args[1]),
TextInputFormat.class, Map1.class);
job1.setReducerClass(Reduce1.class);
job1.setMapOutputKeyClass(Text.class);
job1.setMapOutputValueClass(Text.class);
job1.setOutputKeyClass(Text.class);
job1.setOutputValueClass(NullWritable.class);
job1.setJarByClass(Q11.class);
job1.setOutputFormatClass(TextOutputFormat.class);
FileOutputFormat.setOutputPath(job1, new Path(args[3]));//out1
waitMapperReduce1 = job1.waitForCompletion(true);

boolean waitMapperReduce2 = false;
 
 
if(waitMapperReduce1){
    
    Configuration conf2 = new Configuration();
Job job2 = new Job(conf2, "job2");
job2.setMapperClass(Map2.class);
job2.setReducerClass(Reduce2.class);
job2.setMapOutputKeyClass(Text.class);
job2.setMapOutputValueClass(DoubleWritable.class);
job2.setOutputKeyClass(Text.class);
job2.setOutputValueClass(DoubleWritable.class);
job2.setJarByClass(Q11.class);
job2.setInputFormatClass(TextInputFormat.class);
job2.setOutputFormatClass(TextOutputFormat.class);
FileInputFormat.addInputPath(job2, new Path(args[3]));
FileOutputFormat.setOutputPath(job2, new Path(args[4]));//out2
waitMapperReduce2 = job2.waitForCompletion(true);
}
boolean waitMapperReduce3 = false;
if (waitMapperReduce2) {
Configuration conf3 = new Configuration();
Job job3 = new Job(conf3, "job3");
job3.setMapperClass(Map3.class);
job3.setReducerClass(Reduce3.class);
job3.setMapOutputKeyClass(NullWritable.class);
job3.setMapOutputValueClass(Text.class);
job3.setOutputKeyClass(Text.class);
job3.setOutputValueClass(DoubleWritable.class);
job3.setJarByClass(Q11.class);
job3.setInputFormatClass(TextInputFormat.class);
job3.setOutputFormatClass(TextOutputFormat.class);
FileInputFormat.addInputPath(job3, new Path(args[4]));
FileOutputFormat.setOutputPath(job3, new Path(args[5]));//out3
waitMapperReduce3 = job3.waitForCompletion(true);
}
boolean waitMapperReduce4=false;
if (waitMapperReduce2 && waitMapperReduce3) {
Configuration conf4 = new Configuration();
Job job4 = new Job(conf4, "job4");
MultipleInputs.addInputPath(job4, new Path(args[2]),
TextInputFormat.class, Map4.class);
MultipleInputs.addInputPath(job4, new Path(args[5]),
TextInputFormat.class, Map5.class);
job4.setReducerClass(Reduce4.class);
job4.setMapOutputKeyClass(Text.class);
job4.setMapOutputValueClass(Text.class);
job4.setOutputKeyClass(Text.class);
job4.setOutputValueClass(Text.class);
job4.setJarByClass(Q11.class);
job4.setOutputFormatClass(TextOutputFormat.class);
FileOutputFormat.setOutputPath(job4, new Path(args[6]));//output
waitMapperReduce4 = job4.waitForCompletion(true);
}
}
}
