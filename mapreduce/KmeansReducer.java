package mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class KmeansReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//key is in form artist, title, tempo, year, key, loudness, time_signature
		// values is in form artist, title, tempo, year, key, loudness, time_signature <=> SimilarityScore
		if (!key.toString().isEmpty()) {
			double newCenter = 0.0;
			int size = 0;
			StringBuilder sb = new StringBuilder();
			String artistTitle = "";
			ArrayList<Text> list = new ArrayList<Text>();
			for (Text val: values) {
				list.add(val);
				String [] temp = val.toString().split("<=>");
				//System.out.println("For center " + key.toString() + " The songs that are similar are " + temp[0]);
				double simScore = Double.parseDouble(temp[1]);
				//System.out.println("Its similarity value is " + simScore);
				newCenter += simScore;
				size++;
			}
			newCenter /= size;
			System.out.println("For center " + key.toString() + " The new center point is " + newCenter);
			double lowest = 10.0;
			String lowestString = "";
			for (Text val : list) {
				String [] temp = val.toString().split("<=>");
				String [] info = temp[0].split("\t");//
				String genre = key.toString().split(",")[0];//
				double difference = Math.abs(newCenter - Double.parseDouble(temp[1]));
				if (difference < lowest) {
					lowest = difference;
					//lowestString = temp[0];
					String [] temp2 = info[0].split(",");//
					lowestString = String.format("%s, %s, %s, %s, %s, %s	%s", genre, temp2[2], temp2[3], temp2[4], temp2[5], temp2[6], info[1]);// 
					artistTitle = temp2[0] + temp2[1];
				}
				
			}
			//Configuration conf = context.getConfiguration();
			//String line = conf.get("centers");
			System.out.println("For center " + key.toString() + " The new center point is " + artistTitle);
			context.write(new Text(lowestString), new Text("<===>"));
		}
	}
}
