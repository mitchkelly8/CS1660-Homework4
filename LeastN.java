import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LeastN {
	
	public static class least_5_Mapper extends Mapper<Object, Text, Text, LongWritable> {
		
		private Text word = new Text(); 
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while(itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, new LongWritable(-1));
			}
		}
	}
	
	public static class least_5_Reducer extends Reducer<Text, LongWritable, LongWritable, Text> {

		private TreeMap<Long, String> tmap2;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			tmap2 = new TreeMap<Long, String>();
		}
		
		@Override 
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			String name = key.toString(); 
			long count = 0; 
			
			for(LongWritable val:values) {
				count += val.get();
			}
			
			tmap2.put(count, name); 
			
			if(tmap2.size() > 5) {
				tmap2.remove(tmap2.firstKey());
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			
			for (Map.Entry<Long, String> entry : tmap2.entrySet()) {
				long count = entry.getKey();
				count = count * -1;
				String name = entry.getValue(); 
				context.write(new LongWritable(count), new Text(name));
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
        
		long startTime = System.currentTimeMillis();
		
		Configuration conf = new Configuration(); 
        String[] otherArgs = new GenericOptionsParser(conf, 
                                  args).getRemainingArgs(); 
  
        // if less than two paths  
        // provided will show error 
        if (otherArgs.length < 4)  
        { 
            System.err.println("Error: please provide four paths"); 
            System.exit(2); 
        } 
  
        Job job = Job.getInstance(conf, "top 10"); 
        job.setJarByClass(LeastN.class); 
  
        job.setMapperClass(least_5_Mapper.class); 
        job.setReducerClass(least_5_Reducer.class); 
        
        job.setNumReduceTasks(1);
  
        job.setMapOutputKeyClass(Text.class); 
        job.setMapOutputValueClass(LongWritable.class); 
  
        job.setOutputKeyClass(LongWritable.class); 
        job.setOutputValueClass(Text.class); 
  
        FileInputFormat.addInputPath(job, new Path(otherArgs[0])); 
        FileInputFormat.addInputPath(job, new Path(otherArgs[1])); 
        FileInputFormat.addInputPath(job, new Path(otherArgs[2])); 
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3])); 
        
        System.exit(exitWithTime(job, startTime));
	}
	
	private static int exitWithTime(Job inputJob, long inputStartTime) throws Exception {
		int result = inputJob.waitForCompletion(true) ? 0 : 1;
		long endTime = System.currentTimeMillis();
		System.out.println("LeastN Execution Time (ms): " + (endTime - inputStartTime));	
		return result;
	}

}
