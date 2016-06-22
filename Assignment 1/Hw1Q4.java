import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


@SuppressWarnings("deprecation")
public class Hw1Q4 {
	
	public static TreeMap<Double,ArrayList<String>> UsersByFriendsAge=new TreeMap<Double,ArrayList<String>>(Collections.reverseOrder());
	//The Mapper classes and  reducer  code
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text>{
		

		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			// TODO Auto-generated method stu
			super.setup(context);
		}


		private Text userId=new Text();
		private Text friendsList = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split("\t");
			if(fields.length <2)
				return;
			//System.out.println("Field length ***************  " + fields.length);
			userId.set(fields[0].trim());
			friendsList.set(fields[1].trim()+ "A");
			context.write(userId, friendsList);

		}
	}


	public static class Map2 extends Mapper<LongWritable, Text, Text, Text>{
		private Text userId = new Text();
		private Text age = new Text(); 
		
		//Function to calculate age
				int calcualteAge(String dob)
				{
					//Since we do not know the age, substract the year from 2016
					String[] dobFields = dob.split("/");
					String year =  dobFields[2];
					int age = 2016 - Integer.parseInt(year);
					return age;
				}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");
			userId.set(fields[0]);
			String dob = fields[9].trim();
			Integer intAge = calcualteAge(dob);
			age.set(intAge.toString());
			context.write(userId, age);

		}
	}
	
	//The reducer class	
	public static class Reduce1 extends Reducer<Text,Text,Text,Text> {
		private Text userId = new Text();
		private Text friendListAndAge = new Text();
		//note you can create a list here to store the values
		
		public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {
			
			ArrayList<String> ageAndfriendsList=new ArrayList<String>();
			String details = "";
			String temp = "";
			
			int flag=0;
			int count = 0;
			//Generate details as list of friends | age of the user"
			for (Text value : values) {
				
				if(value.toString().contains("A"))
				{
					if(temp.length() < 1)
					{
						details = value.toString() + ":";
					}
					//if already have something in temp generate details
					else
					{
						details = value.toString() + ":" + temp;
					}
					count++;
				}
				else
				{
					if(details.length() < 1)
					{
						temp = value.toString();
						continue;
					}
					
					else
					{
					details += value.toString();
					}
					count++;
				}
				
			}
			userId.set(key.toString());
			friendListAndAge.set(details);
			if(count == 2)
			context.write(userId, friendListAndAge);
			
						
		}		
	}
	
	public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {

        private Text age = new Text();
        private Text userId = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
        		// Split the line and for each friend Id output Friend Id as key and age as value
        		String[] linesplit = value.toString().split("\t");
        		String[] userIdAndAge = linesplit[1].split(":");
        		//System.out.println("The line split[1]" + userIdAndAge[1] );
        		String[] userIdList = userIdAndAge[0].split(",");
        		String tempAge = userIdAndAge[1];
        		//System.out.println("user Id list : "+userIdAndAge[0] + "Age : "+ IntegerAge );
        		for (int i =0;i<userIdList.length; i++)
        		{
        			String correctUserId = userIdList[i].replace("A", "");
        			userId.set(correctUserId);
        			age.set(tempAge);
        			context.write(userId, age);
        		//	System.out.println("The Id is " + userId.toString() + "******************" + age.toString());
        			
        		}
        }
    }
	
	public static class Map4 extends Mapper<LongWritable,Text,Text,Text> 
	{
		
		//345
		private Text userId = new Text();
		private Text  nameAndAddress = new Text();
		
		@Override
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException
		{
			String[] line = value.toString().split(",");
			String id = line[0];
			String name = line[1];
			String streetAddress = line[3];
			String city = line[4];
			String state = line[5];
			
			String address = streetAddress + "," + city+ "," + state;
			userId.set(id);
			nameAndAddress.set(name + "," + address + "flagA");
			context.write(userId, nameAndAddress);
			//System.out.println("output name And Adress " + nameAndAddress.toString());
		}
	}
	
	public static class Reducer2 extends Reducer<Text, Text, Text, DoubleWritable> {

        
		@Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        	
        	double sumAge = 0;
            double count = 0;
            double average = 0;
            String nameAndAdress = "";

            for (Text val: values) { 
            	if(val.toString().contains("flagA"))
            	{
            		nameAndAdress = val.toString();
            		nameAndAdress = nameAndAdress.replace("flagA", "");
            		continue;
            	}
                    sumAge += Double.parseDouble(val.toString());
                    count++;
                }
            
            // getting the average of friends for each user.
            average = (double)(sumAge / count);
            
            if(!Double.isNaN(average))
            {
            	//If user doesnt exist create the spot in the tree map
            if(UsersByFriendsAge.get(average)==null)
            {
            	ArrayList<String> usersList=new ArrayList<String>();
            	usersList.add(key.toString()+ "::" + nameAndAdress);
            	//System.out.println("name and address " + nameAndAdress) ;
            	UsersByFriendsAge.put(average, usersList);
            	
            }
            else{
            	//Add the user to the list of users having same average.
            	ArrayList<String> usersList=UsersByFriendsAge.get(average);
            	usersList.add(key.toString() + "::" + nameAndAdress);
            	UsersByFriendsAge.put(average, usersList);
            }
            }
            
        }
        
		public void cleanup(Context context) throws IOException, InterruptedException {
			Text userNameAndAdress = new Text();
			DoubleWritable averageFriendsAge = new DoubleWritable();
			Set<Double> ages = UsersByFriendsAge.keySet();
			
			int count = 0;
			
			for(Double age : ages)
			{
				ArrayList<String> userDetails = UsersByFriendsAge.get(age);
				
				for (String userDetail:userDetails)
				{
					//System.out.println("User detail : " + userDetail);
					if(count == 20 ) break;
					String[] fields = userDetail.split("::");
					System.out.println("final field is " + fields[1]);
					String nameAndAdress = fields[1];
					userNameAndAdress.set(nameAndAdress);
					averageFriendsAge.set(age);
					context.write(userNameAndAdress, averageFriendsAge);
					count++;
				}
			}
		}
		
		
 /*      public void cleanup(Context context) throws IOException, InterruptedException
       {
    	   IntWritable key2=null;
    	   Text title1=new Text();
    	   
    	   Set<Double> finalresult=top.keySet();
    	   int count=0;
    	   Outer:for(Double key : finalresult)
    	   {
    		   ArrayList<String> title=top.get(key);
    		   
    		   for(String temp: title)
    		   {
    			   if(count==5)
        		   {
        			   break Outer;
        		   }
    		   String kiran=temp+"\t"+key.toString();
    		   title1.set(kiran);
    		   
    		   context.write(key2,title1);
    		   count++;
    		   
    		   }
    	   }
    	   
       } */
    	
    }

	//Driver code
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		/*String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 4) {
			System.err.println("Usage: JoinExample <in> <in2> <IntermediateOutput> <in3> <finaloutput>");
			System.exit(2);
		}*/

		// create a job with name "joinexc" 
		
		Job job = new Job(conf, "Combine age and friends list"); 
		job.setJarByClass(Hw1Q4.class);
		job.setReducerClass(Reduce1.class);
		
		// OPTIONAL :: uncomment the following line to add the Combiner
		// job.setCombinerClass(Reduce.class);


		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class ,
				Map1.class );

		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class,Map2.class );

		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);

		//set the HDFS path of the input data
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		//job.waitForCompletion(true);
		job.waitForCompletion(true);

            JobConf conf2 = new JobConf();
            Job job2 = new Job(conf2, "job2");
            
            MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, Map3.class);
            MultipleInputs.addInputPath(job2, new Path(args[1]),TextInputFormat.class,Map4.class );
 //           MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, Movies.class);
                
            job2.setReducerClass(Reducer2.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoubleWritable.class);
            job2.setJarByClass(Hw1Q4.class);

           // job2.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));

            job2.waitForCompletion(true);

        
	}
}
