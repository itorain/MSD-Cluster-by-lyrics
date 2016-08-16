package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IDFMapClass extends Mapper<LongWritable, Text, Text, Text> {
	
	private static final Pattern brackets = Pattern.compile("\\{(.*?)\\}");
	
	//public void setup()
	// Input comes in as {artist_name, title, tempo, year, energy, key, loudness, time_signature}[4] {'word':4, ...}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		List<String> list = new ArrayList<String>();
		Matcher m = brackets.matcher(value.toString());
		while (m.find()) {
		    list.add(m.group(1));
		}
		String [] line = list.toArray(new String[list.size()]);
		String songInfo = line[0];
		String [] words = line[1].split(",");
		ArrayList<Integer> counts = new ArrayList<Integer>();
		for (int i = 0; i < words.length; i++) {
			String [] wordCount = words[i].split(":");
			counts.add(Integer.parseInt(wordCount[1].trim()));
		}
		int max = Collections.max(counts);
		for (int i = 0; i < words.length; i++) {
			String [] wordCount = words[i].split(":");
			double count = Double.parseDouble(wordCount[1]);
			count /= (double)max;
			//String val = wordCount[1] + '\t' + songInfo.replaceAll("\t", " ");
//			StringBuilder sb = new StringBuilder();
//			sb.append(count).append("\t").
			String val = count + "\t" + songInfo.replaceAll("\t", " ");
			context.write(new Text(wordCount[0].replaceAll("'", "")), new Text(val));
		}
		//context.write(new Text(line[0]), new Text(line[1]));
	}
}