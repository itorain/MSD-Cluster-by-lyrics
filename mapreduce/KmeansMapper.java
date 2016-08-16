package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class KmeansMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	
	public Double calculateEuclidean(String song, String center) {
		double n = 0.0;
		
		String [] songAttributes = song.split(",");
		String [] centerAttributes = center.split(",");
		//System.out.printf("The song is %s tempo is %s, the year is %s, the key is %s, the loudness is %s, and the singature is %s\n"
				//,centerAttributes[0], centerAttributes[1], centerAttributes[2], centerAttributes[3], centerAttributes[4], centerAttributes[5]);
		double songTempo;
		double centerTempo;
		double songYear;
		double centerYear;
		double songKey;
		double centerKey;
		double songLoudness;
		double centerLoudness;
		double songSig;
		double centerSig;
		try {
			songTempo = Double.parseDouble(songAttributes[2].trim()); 
			centerTempo = Double.parseDouble(centerAttributes[1].trim());
			songYear = Double.parseDouble(songAttributes[3].trim());
			centerYear = Double.parseDouble(centerAttributes[2].trim());
			songKey = Double.parseDouble(songAttributes[4].trim());
			centerKey = Double.parseDouble(centerAttributes[3].trim());
			songLoudness = Double.parseDouble(songAttributes[5].trim());
			centerLoudness = Double.parseDouble(centerAttributes[4].trim());
			songSig = Double.parseDouble(songAttributes[6].trim());
			centerSig = Double.parseDouble(centerAttributes[5].trim());
		}
		catch (Exception e) {
			return 0.0;
		}
		
		if (songYear == 0.0 || centerYear == 0.0) {
			songYear = 0.0;
			centerYear = 0.0;
		}
		
		n = Math.pow((centerTempo - songTempo), 2) + Math.pow((centerKey - songKey), 2) + Math.pow((centerLoudness - songLoudness), 2) + Math.pow((centerSig - songSig), 2);
		n = Math.sqrt(n);
		
		return n;
	}
	
	public double calculateMagnitude(HashMap<String, Double> hm) {
		double n = 0.0;
		
		for (double tfidf : hm.values()) {
			n += Math.pow(tfidf, 2);
		}
		
		return Math.sqrt(n);
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// input value is in form artist, title, tempo, year, key, loudness, time_signature \t word:tfidf? ...
		// input from centers string is in form rtist, title, tempo, year, key, loudness, time_signature \t word:tfidf? ... <===>
		Configuration conf = context.getConfiguration();
		String line = conf.get("centers");
		ArrayList<String> clist = new ArrayList<String>(Arrays.asList(line.split("<===>")));
		
		String [] songInfo = value.toString().split("\t");
		String [] wordScores = songInfo[1].split("<==>");
		HashMap<String, Double> lyrics = new HashMap<String, Double>();
		for (int i = 0; i < wordScores.length; i++) {
			String [] temp = wordScores[i].split(":");
			lyrics.put(temp[0], Double.parseDouble(temp[1]));
		}
		double mostSimilar = 0.0;
		double minDistance = 200000.0;
		String minCenter = "";
		for (String center : clist) {
			String [] centerInfo = center.split("\t");
			if (!centerInfo[0].equals(" ")) {
				String [] cWordScores = centerInfo[1].split("<==>");
				HashMap<String, Double> clyrics = new HashMap<String, Double>();
				for (int i = 0; i < cWordScores.length; i++) {
					//System.out.println(cWordScores[i]);
					String [] temp = cWordScores[i].split(":");
					//System.out.println(cWordScores[i]);
					clyrics.put(temp[0], Double.parseDouble(temp[1]));
				}
				
				double cosineSimilarity = 0.0;
				double asqr = 0.0;
				double bsqr = 0.0;
				double attributeSimilarity = calculateEuclidean(songInfo[0], centerInfo[0]);
				if (attributeSimilarity != 0.0) {
					attributeSimilarity = 1.0/attributeSimilarity;
				}
				else {
					attributeSimilarity = 1.0;
				}
				//double cosineSimilarity = calculateEuclidean(songInfo[0], centerInfo[0]);
				//for every word in the center since we only care about how similar a song is to a center
				for (String word : clyrics.keySet()) {
					if (lyrics.containsKey(word)) {
						double aVal = lyrics.get(word);
						double bVal = clyrics.get(word);
						//asqr += Math.pow(aVal, 2);
						//bsqr += Math.pow(bVal, 2);
						cosineSimilarity += (aVal * bVal);
					}
					else {
						// figure out a way to weight words that dont appear
					}
				}
				//asqr = Math.sqrt(asqr);
				//bsqr = Math.sqrt(bsqr);
				asqr = calculateMagnitude(lyrics);
				bsqr = calculateMagnitude(clyrics);
				cosineSimilarity /= (asqr * bsqr);
				//System.out.println("The song is " + songInfo[0] + " For center " + centerInfo[0] + " The value of cosine similarity is " + cosineSimilarity + " and the attribute similarity is " + attributeSimilarity);
				cosineSimilarity *= attributeSimilarity;
				if (cosineSimilarity > mostSimilar) {
					mostSimilar = cosineSimilarity;
					minCenter = centerInfo[0];
				}
//				System.out.println("The song is " + songInfo[0] + " For center " + centerInfo[0] + " The value of cosine similarity is " + cosineSimilarity + " and the attribute similarity is " + attributeSimilarity);
			}
		}
		
		context.write(new Text(minCenter), new Text(value.toString() + "<=>" + mostSimilar));
	}
}
