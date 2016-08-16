package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FormatMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String [] line = value.toString().split("\t"); //input is song info \t word \t tfidf
		String songInfo = line[0]; // 
		String wordTFIDF = line[1] + ':' + line[2];
		context.write(new Text(songInfo), new Text(wordTFIDF));
	}
}