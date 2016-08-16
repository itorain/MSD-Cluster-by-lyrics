package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class IDFReduceClass extends Reducer<Text, Text, Text, Text>{ //number of songs in test set is 2350 and number of songs in msd is 990036
	
	public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		ArrayList<Double> tfscores = new ArrayList<Double>();
		ArrayList<String> songsInfo = new ArrayList<String>();
		//int numOfSongs = 2350;
		int numOfSongs = 237660;
		int subCollectionCount = 0;
		for (Text val: values) {
			//System.out.println("The key is " + key.toString());
			//System.out.println("The value is " + val.toString());
			//System.out.println(val.toString());
			String [] temp = val.toString().split("\t");
			tfscores.add(Double.parseDouble(temp[0]));
			songsInfo.add(temp[1]);
			subCollectionCount++;
		}
		for (int i = 0; i < tfscores.size(); i++) {
			//System.out.println("the total number of authors is: " + N + " the total number of subcollections is: " + subCollectionCount + " the tf score for " + key.toString() + " is: " + tfscores.get(i));
			//double idf = (double)Math.log((double)numOfSongs/(double)subCollectionCount) / (double)Math.log(2);
			double idf = (double)Math.log((double)numOfSongs/(double)subCollectionCount);
			idf += 1.0;
			double num = idf * tfscores.get(i);
			String Keyval = songsInfo.get(i);
			String val = key.toString() + '\t' + String.format("%.12f", num);
			context.write(new Text(Keyval), new Text(val));
		}
		//context.write(arg0, arg1);
	}
}