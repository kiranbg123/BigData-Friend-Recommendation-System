import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Hw1Q3 {

	public static class FriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text friendPair = new Text();
		private Text personFriends = new Text();
		private Text tempKey = new Text();
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Split by tab the user and its friends list. Seperate the key
			// value pair
			String eachLine = value.toString();
			/*String[] split = eachLine.split("\t");
			String userName = split[0];
			String[] friends = Arrays.copyOfRange(split, 1, split.length); */
			
			//trying out new way of handling each record
			//Take 2 users provided
			Configuration conf = context.getConfiguration();
			String user1 = conf.get("user1");
			String user2 = conf.get("user2");
			int index = eachLine.indexOf('\t');
		      if(index == -1) 
		       return;        
		      String userName = eachLine.substring(0, index);
		      ArrayList<String> friendList = new ArrayList<String>();
		      StringTokenizer tokenizer = new StringTokenizer(eachLine.substring(index+1), ""+',');
		      //int i = 0;
		      while(tokenizer.hasMoreTokens()) {
		       friendList.add(tokenizer.nextToken());
		      }

		      
		      
		     tempKey.set(user1+" "+user2);
		    if(userName.equalsIgnoreCase(user1) || userName.equalsIgnoreCase(user2)) {
		    	  //for (String eachFriend : friendList) {
						String friends = eachLine.replace(userName + "\t", "");
						//String id = userName.compareTo(eachFriend) < 0
							//	? userName + " " + eachFriend
							//	: eachFriend + " " + userName;
						//friendPair.set();
						personFriends.set(friends);
						//System.out.println(friendPair.toString() + ":"
							//+ personFriends.toString());
						//if((userName==user1 && eachFriend == user2) || (userName == user2 && eachFriend == user1))
						context.write(tempKey, personFriends);
						System.out.println(tempKey.toString() + " -->" + personFriends.toString());
					//}
		    	
		      } 
			// For each friend in the list output (user, list of friends)
		/**for (String eachFriend : friendList) {
				String friends = eachLine.replace(userName + '\t', "");
				String id = userName.compareTo(eachFriend) < 0
						? userName + " " + eachFriend
						: eachFriend + " " + userName;
				friendPair.set(id);
				personFriends.set(friends);
				//System.out.println(friendPair.toString() + ":"
					//+ personFriends.toString());
				//if((userName==user1 || eachFriend == user2) && (userName == user2 || eachFriend == user1))
				//System.out.println("Username: " + userName + "");
				//if(userName == user1)
				context.write(friendPair, personFriends);
			} **/
		}
	}
	
	public static class Mapjoin_mapper extends Mapper<LongWritable, Text, Text, Text> {

		static String intermediateOutputLine = "";
		static HashMap<String, String> map = new HashMap<String, String>();
		

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			URI[] files = context.getCacheFiles();

			if (files.length == 0) {
				throw new FileNotFoundException("file not found in distributed Cache");
			}
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream in = fs.open(new Path(files[0]));
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			readCacheFile(br);
		};

		private void readCacheFile(BufferedReader br) throws IOException {
			String line = br.readLine();
			while (line != null) {
				String[] fields = line.split("\t");
				String[] friends = fields[1].split(",");
				// Put Id as a key and name:zipcode as the value
				for(int i =0;i < friends.length; i++)
				{
					map.put(friends[i].trim(), "Ignore");
				}
				line = br.readLine();
			}
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			Text userIds = new Text();
			Text nameAndZipCode = new Text();
			Configuration conf = context.getConfiguration();
			String user1 = conf.get("user1");
			String user2 = conf.get("user2");

			intermediateOutputLine = value.toString();
			String[] fields = intermediateOutputLine.split(",");
			//Field seven contains the ids of mutual friend
			String[] friendDetails = fields[1].split(",");
			//Find the zip code and name for each friend
			String userId = fields[0];
			String firstName = fields[1];
			String zipCode = fields[6];
			String details = "";
			if(map.containsKey(userId))
			{
			details += firstName +":" + zipCode;
			userIds.set(user1 + " " + user2);
			nameAndZipCode.set(details);
			context.write(userIds, nameAndZipCode);
			System.out.println(userIds.toString() + " " + nameAndZipCode.toString());
			}
			
		}
	}
	
	public static class FriendDetailsReducer extends Reducer<Text,Text,Text,Text>
	{
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Text friendsDetail = new Text();
			String detail ="[";
			for (Text value:values)
			{
				detail += value.toString() + ",";
			}
			//Remove an extra comma at at the end
			detail = detail.substring(0, detail.length()-1);
			detail += "]";
			friendsDetail.set(detail);
			context.write(key,friendsDetail);
			System.out.println(key.toString() + " " + friendsDetail.toString());
			
			
		}
	}

	public static class FriendsReducer extends Reducer<Text, Text, Text, Text> {
		private Text mutualFriends = new Text();

		// Calculates the common friends in the two list
		private String commonFriendsFinder(String[] s1, String[] s2) {
			HashSet<String> h1 = new HashSet<String>();
			HashSet<String> h2 = new HashSet<String>();

			for (int i = 0; i < s1.length; i++) {
				h1.add(s1[i]);
			}
			for (int i = 0; i < s2.length; i++) {
				h2.add(s2[i]);
			}

			h1.retainAll(h2);
			String[] res = h1.toArray(new String[0]);
			String intersect = new String("");
			for (int i = 0; i < res.length; i++) {
				intersect += res[i] +",";
			}

			/*
			 * char[] letters = intersect.toCharArray(); Arrays.sort(intersect);
			 * String sortedIntersect = new String(letters);
			 */
			return intersect;
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// Keep the combined friends as an array
			String[][] combined = new String[2][];
			int cur = 0;
			for (Text value : values) {
				String friends = value.toString();
				String[] frinedList = friends.split(",");
				combined[cur++] = frinedList;
			}
			/*
			 * Trying out some new stuff.. testing
			 * 
			 * 
			 * for(Text key : keys) { ; }
			 */

			// Calculate the common friends and put them in the mutual friends.
			//If no list is empty then only call common friends finder
			if(combined[0] != null && combined[1] != null)
			{
				
			mutualFriends.set(commonFriendsFinder(combined[0], combined[1]));
			
			}
			else
			{
			
				mutualFriends.set("One of them has no friends");
			}
			context.write(key, mutualFriends);
			System.out.println(mutualFriends.toString());
		}
	}

	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		conf.set("user1", args[4]);
		conf.set("user2", args[5]);
		Job job = Job.getInstance(conf, "Mutual Friends");
		job.setJarByClass(Hw1Q3.class);
		job.setMapperClass(FriendsMapper.class);
		job.setReducerClass(FriendsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		job.waitForCompletion(true);
		
		//Job2 details
		Job job2 = Job.getInstance(conf,"Mutual Friend Details");
		job2.setJarByClass(Hw1Q3.class);
		job2.setMapperClass(Mapjoin_mapper.class);
		job2.setReducerClass(FriendDetailsReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		//job2.setNumReduceTasks(0);
		
		
		job2.addCacheFile(new URI(args[1]+ "/part-r-00000"));
		FileInputFormat.addInputPath(job2, new Path(args[2]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		//FileInputFormat.setMinInputSplitSize(job2, 150000);		
		job2.waitForCompletion(true);
	}
}