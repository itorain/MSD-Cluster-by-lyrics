## Million Song Dataset K-means cluster songs by lyrics
The Million song dataset is a well known data containing information about a million randomly picked songs. The dataset contains meta
information like tags and tempo about a song. An extension was added to the dataset called the [musiXmatch dataset](http://labrosa.ee.columbia.edu/millionsong/musixmatch). This dataset featured about 250,000 songs from the MSD with the
lyrics presented in a bag-of-words style. The top 5000 words appearing across all the sampled songs was taken and then each song was given
a count of how many times one of the top words appeared in the song. Unfortunately this data was not as useful as originally intended
because of the top 5000 words containing words like a, the, etc and those generally being the most common words. The idea for this project
was to try to create a genre categorizing algorithm, that would take the meta information and lyrics and categorize the songs into 
clusters that could then be labeled genres. Initial cluster centers were picked and then the songs were clustered using k-means clustering
and cosine similarity as the measure of how similar two songs were lyrically. The MSD was ran through this program to produce plain
text file inputs, and then the fixData.py was used to further clean up those text files and pair songs using their id to their mathcing
lyrics from the musixmatch dataset. Finally, the k-means map reduce program was run. 
**NOTE** This repo is still under development. Sample input files will be able as an example

## How to run
* Have a hadoop cluster up and running or emr in Amazon.
* create jar from mapreduce classes and MainClass.java as the main driver for the jar
* Add inputs to hdfs (*Sample inputs coming soon*)
* Run as MainClass centers inputDir outputDir
