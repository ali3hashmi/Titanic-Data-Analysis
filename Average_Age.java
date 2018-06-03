import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Average_Age {
	
	public static class AverageMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		
		private Text gender = new Text();
		private IntWritable age =new IntWritable();
		
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
		
			String line =value.toString();
			String[] str=line.split(",");
			if(str.length>6){
				gender.set(str[4]);
				
				if(str[1].equals("0")){
					
					if(str[5].matches("\\d+")){
						int i = Integer.parseInt(str[5]);
						age.set(i);
					}
				}
			}
			context.write(gender, age);
		}
	}
	
	public static class AverageReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
			
			int sum=0;
			int i=0;
			for(IntWritable val:values){
				i +=1;
				sum +=val.get();
			}
			sum = sum/i;
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		Job job =Job.getInstance(conf,"Average_Age");
		job.setJarByClass(Average_Age.class);
		job.setMapperClass(AverageMapper.class);
		job.setCombinerClass(AverageReducer.class);
		job.setReducerClass(AverageReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)? 0 : 1);
		
		
		
		
		
		
	}

}
