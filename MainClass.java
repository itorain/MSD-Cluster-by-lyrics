import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MainClass {

	public static void main(String[] args) throws Exception { //args 0: input file 1: output directory 2: centers.txt
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(new Configuration(true));
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(args[2]))));
		String line = br.readLine();
		//String [] centers = line.split("<===>");
		//System.out.println(centers[0] + "\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
		conf.set("centers", line);
		Job job = Job.getInstance(conf);
		Path output = new Path(args[1]);
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }
		     
	    job.setJarByClass(MainClass.class);
	    job.setMapperClass(mapreduce.IDFMapClass.class);
	    job.setReducerClass(mapreduce.IDFReduceClass.class);

	    job.setInputFormatClass(TextInputFormat.class);
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, output);
	    
	    job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		job.waitForCompletion(true);

		Job job2 = Job.getInstance(conf);
		Path output2 = new Path("/reformatOutput");
		if (hdfs.exists(output2)) {
            hdfs.delete(output2, true);
        }
		
		FileInputFormat.addInputPath(job2, output);
		FileOutputFormat.setOutputPath(job2, output2);
		job2.setJarByClass(MainClass.class);
		job2.setMapperClass(mapreduce.FormatMapper.class);
		job2.setReducerClass(mapreduce.FormatReducer.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		job2.waitForCompletion(true);
		
		Job job3 = Job.getInstance(conf);
		Path output3 = new Path("/kmeansOutput");
		if (hdfs.exists(output3)) {
            hdfs.delete(output3, true);
        }
		
		FileInputFormat.addInputPath(job3, output2);
		FileOutputFormat.setOutputPath(job3, output3);
		job3.setJarByClass(MainClass.class);
		job3.setMapperClass(mapreduce.KmeansMapper.class);
		job3.setReducerClass(mapreduce.KmeansReducer.class);
		
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		
		job3.waitForCompletion(true);
		boolean success = false;
		int iteration = 1;
		while (iteration < 3) {
			StringBuilder sb = new StringBuilder();
			Path p = new Path("/kmeansOutput/part-r-00000");
			BufferedReader br1 = new BufferedReader(new InputStreamReader(hdfs.open(p)));
			String temp = br1.readLine();
			while (temp != null) {
				sb.append(temp);
				temp = br1.readLine();
				//System.out.println(temp);
			}
			//System.out.println(sb.toString());
			conf.set("centers", sb.toString());
			Job njob = Job.getInstance(conf);
			if (hdfs.exists(output3)) {
				hdfs.delete(output3, true);
			}
			FileInputFormat.addInputPath(njob, output2);
			FileOutputFormat.setOutputPath(njob, output3);
			njob.setJarByClass(MainClass.class);
			njob.setMapperClass(mapreduce.KmeansMapper.class);
			njob.setReducerClass(mapreduce.KmeansReducer.class);
				
			njob.setOutputKeyClass(Text.class);
			njob.setOutputValueClass(Text.class);
			njob.setInputFormatClass(TextInputFormat.class);
			njob.setOutputFormatClass(TextOutputFormat.class);
			
			success = njob.waitForCompletion(true);
			iteration++;
		}
		
		//Path p = new Path("/kmeansOutput/part-r-00000");
		//BufferedReader br1 = new BufferedReader(new InputStreamReader(hdfs.open(p)));
		//String temp = br1.readLine();
		System.out.println("\n\n\n\\n\n\n\n\n");
		System.out.println("Finally Done");
		
		System.exit(success ? 0:1);
	}

}
