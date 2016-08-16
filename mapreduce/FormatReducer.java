package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

public class FormatReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		for (Text val : values) {
			//String [] temp = val.toString().split("\t");
			String temp = val.toString();
			sb.append(temp);
			sb.append("<==>");		
		}
		//System.out.println(sb.toString());
		context.write(key, new Text(sb.toString()));
	}
}
