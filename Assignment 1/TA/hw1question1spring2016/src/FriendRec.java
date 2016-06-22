// hadoop jar testFr.jar FriendRec /testnet /testnet/mynet.txt /hw120161
// hadoop jar testFr.jar FriendRec /socNetData/networkdata /socNetData/networkdata/soc-LiveJournal1Adj.txt /hw12016FA
//gbaduz@gbaduz-ThinkPad-T410:~/Desktop$ scp testFr.jar gga110020@cs6360.utdallas.edu:

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FriendRec{
	

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private Text Queryuserid = new Text();  // type of output key 
		private Text nonfriend = new Text();
		
		HashMap<String,String> myMap;
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//from ratings
			
			String[] mydata = value.toString().split("\t");
			if(mydata.length > 1 && mydata[1].length()!=0) {
			String currentuser = mydata[0];
			String[] currentuserfriends = mydata[1].split(",");
			Configuration conf = context.getConfiguration();
			//String Quserid = conf.get("userid");
			
			
			for(String Quserid : myMap.keySet() ){
				String QuserFriends = myMap.get(Quserid.trim());
			String[] QuserFriendsList = QuserFriends.split(",");
			
			for (String s: QuserFriendsList){
				
				if (currentuser.trim().compareTo(s.trim())== 0){//if current user is a part of the query user friend list, then they are friends
					
					//if they are friends then find friend of current user who are not friends of query user.
					List<String> currUserFriendlist = new ArrayList<String>(Arrays.asList(currentuserfriends));
					List<String> QuserFriendsAList = new ArrayList<String>(Arrays.asList(QuserFriendsList));
					
					currUserFriendlist.removeAll(QuserFriendsAList);
					
					for (String f: currUserFriendlist){
						Queryuserid.set(Quserid);
						nonfriend.set(f);
						context.write(Queryuserid ,nonfriend);
					}
					
					break;
				}
				
			}
			
		}
				
			}	
			
			
		}
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stu
			super.setup(context);
			//read data to memory on the mapper.
			myMap = new HashMap<String,String>();
			Configuration conf = context.getConfiguration();
			String mybusinessdataPath = conf.get("userfile");
			//String userid = conf.get("userid");
			//e.g /user/hue/input/
			Path part=new Path("hdfs://cshadoop1"+mybusinessdataPath);//Location of file in HDFS
			
			
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
		    for (FileStatus status : fss) {
		        Path pt = status.getPath();
		        
		        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		        String line;
		        line=br.readLine();
		        while (line != null){
		        	String[] arr=line.split("\t");
		        	//if(arr[0].compareTo(userid)== 0){
		        	if(arr.length > 1 && arr[1].length()!=0){
		            myMap.put(arr[0].trim(), arr[1].trim()); //businessid and the remain datacolumns
		        	}
		            line=br.readLine();
		        }
		       
		    }
			
			
			
	       
		}
		
		
	}

	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		
		public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {
		
		/*	int count=0;
			for(Text t : values){
				count++;
			}
			Text myCount = new Text();
			myCount.set(""+count);
			context.write(key,myCount);*/
			
			HashMap<String,Integer> frequencymap = new HashMap<String,Integer>();
			for(Text a:values) {
			  if(frequencymap.containsKey(a.toString())) {
			    frequencymap.put(a.toString(), frequencymap.get(a.toString())+1);
			  }
			  else{ frequencymap.put(a.toString(), 1); }
			}
			int count = 0;
			Text myCount = new Text();
			String output="";
			for(String t : frequencymap.keySet()){
				if(key.toString().compareTo(t)!=0){
				output = output + " "+t + " "+ frequencymap.get(t);
				
				count++;
				}
				//if (count > 10)break;
			}
			myCount.set(output);
			context.write(key,myCount);
			
		}
	}

// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
		if (otherArgs.length != 3) {
			System.err.println("Usage: FriendRec <infile> <userfile> <out>");
			System.exit(2);
		}
		
	//	DistributedCache.addCacheFile(new URI("hdfs://cshadoop1"+ otherArgs[1]), conf);       
		
		conf.set("userfile", otherArgs[1]);
		//conf.set("userid", otherArgs[3]);
		
		Job job = new Job(conf, "FriendRec");
		job.setJarByClass(FriendRec.class);
		
	//	 final String NAME_NODE = "hdfs://localhost:9000";
    //    job.addCacheFile(new URI(NAME_NODE
	//	    + "/user/hduser/"+otherArgs[1]+"/users.dat"));
	/*	final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
	        job.addCacheFile(new URI(NAME_NODE
			    + otherArgs[1]));
	*/	
	  //    final String NAME_NODE = "hdfs://cshadoop1";
	  //      job.addCacheFile(new URI(NAME_NODE
	//		    + otherArgs[1]));
		
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		//job.setNumReduceTasks(0);
//		uncomment the following line to add the Combiner
//		job.setCombinerClass(Reduce.class);
		
		// set output key type 
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}