import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Hw1Q1 {
/* for each line of input, generates user pairs that have common friend, or are already friends */
  public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);  // one indicates that two users have a common friend
    private final static IntWritable zero = new IntWritable(0); // zero indicates that two users are already friends
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      int index = line.indexOf('\t');
      if(index == -1) 
       return;        
      String user_id = line.substring(0, index);
      ArrayList<String> friends_id = new ArrayList<String>();
      StringTokenizer tokenizer = new StringTokenizer(line.substring(index+1), ""+',');
      while(tokenizer.hasMoreTokens()) {
       friends_id.add(tokenizer.nextToken());
      }
      int length = friends_id.size(), i,j;
      for(i = 0; i < length; i++) {
        context.write(new Text(user_id + ',' + friends_id.get(i)), zero);
      }
      for(i = 0; i < length; i++) {
        for(j = 0; j < length; j ++) {
          if(j == i)
            continue;
          context.write(new Text(friends_id.get(i) + ',' + friends_id.get(j)), one);
        }
      }
    }
  } 
  /* count number of common friends for each pair of user */       
  public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0, prod = 1;
      for (IntWritable val : values) {
        sum += val.get();
        prod *= val.get();
      }
      if( prod!=0 )
        context.write(key, new IntWritable(sum));
    }
  }
 
  /* Use the first ID as key to map each line to a key value pair */
  public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      
      int index, index2;
      if((index = line.indexOf(',')) == -1) 
       return;
      if((index2 = line.indexOf('\t')) == -1)
       return;
      String user_id = line.substring(0, index);
      String friend_id = line.substring(index+1, index2);
      String commonFriend = line.substring(index2+1);
      context.write(new Text(user_id), new Text(friend_id + ',' + commonFriend));
    }
  }
  /* for each user, find the 10 IDs that have most common friends with the user */
  public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
   
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      Vector<int[]> mostCommon  = new Vector<int[]>();
      // add friend id to the mostCommon array in order
      for (Text val : values) {
        String val_str = val.toString();
        int index;
        if((index = val_str.indexOf(',')) == -1)
          return;
        int[] pair_array = new int[2];
        pair_array[0] = Integer.parseInt(val_str.substring(0, index));
        pair_array[1] = Integer.parseInt(val_str.substring(index+1));
      
        if(mostCommon.isEmpty())
          mostCommon.insertElementAt(pair_array,0);
        else {
          int i;
          for(i = 0; i < Math.min(mostCommon.size(), 10); i ++) {
            if(mostCommon.get(i)[1] < pair_array[1] ||
            (mostCommon.get(i)[1] == pair_array[1] && mostCommon.get(i)[0] > pair_array[0])) {
              mostCommon.insertElementAt(pair_array, i);
              while(mostCommon.size() > 10)
                mostCommon.removeElementAt(mostCommon.size()-1);
              break;
            }
          }
          if( i == mostCommon.size() && i < 10)
            mostCommon.add(pair_array);
        }
      }
      String mostCommon_str = "";
      for(int i = 0; i < mostCommon.size(); i ++){
        mostCommon_str += Integer.toString(mostCommon.get(i)[0]);
        if(i != mostCommon.size()-1)
          mostCommon_str += ',';
      }
      context.write(key, new Text(mostCommon_str));
    }  
  }

  public static void main(String[] args) throws Exception {
    // --------- first Map-Reduce Job -------------------------
    // For each user, calculates the number of common friends 
    // with other users
    Configuration conf = new Configuration();

    Job job = new Job(conf, "task 1");
    job.setJarByClass(Hw1Q1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setMapperClass(Map1.class);
    job.setReducerClass(Reduce1.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
    // -------- Second Map Reduce Job -------
    // For each user, find top recommendations
    Configuration conf2 = new Configuration();

    Job job2 = new Job(conf2, "Task 2");
    job2.setJarByClass(Hw1Q1.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(Text.class);

    job2.setMapperClass(Map2.class);
    job2.setReducerClass(Reduce2.class);

    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job2, new Path(args[2]));
    FileOutputFormat.setOutputPath(job2, new Path(args[3]));

    job2.waitForCompletion(true);
  }    

}