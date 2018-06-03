import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer.Context;



 

public class People_Survived {
	
	public static class PeopleMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		
		private Text people = new Text();
		private final static IntWritable one = new IntWritable(1);
		
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			
			String line=value.toString();
			String[] str =line.split(",");
			
			if(str.length>6){
				String survived = str[1]+" "+str[4]+" "+str[5];
				people.set(survived);
				context.write(people, one);
				
			}
		}
	}
	
	public static class PeopleReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
			
			int sum =0;
			for(IntWritable val:values){
				
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		Job job =Job.getInstance(conf,"Average_Age");
		job.setJarByClass(People_Survived.class);
		job.setMapperClass(PeopleMapper.class);
		job.setCombinerClass(PeopleReducer.class);
		job.setReducerClass(PeopleReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)? 0 : 1);
		

	}

}
