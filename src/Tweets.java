import java.util.Arrays;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Set;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;


class MasterThread extends Thread {

	public static final int NUM_LINES = 100;
	public static int tweetNum;
	public static String file;
	public String[] block;
	public static AtomicBoolean readAll;
	public static AtomicInteger workersFinished;

	public MasterThread() {
		block = new String[NUM_LINES];
		tweetNum = 0;
		readAll = new AtomicBoolean(false);
		workersFinished = new AtomicInteger();
	}

	public void run() {

		FileReader fr = null;
		BufferedReader br = null;
		
		try {
			fr = new FileReader(file);
			br = new BufferedReader(fr);


			int i;
			String tweet;
			boolean doneReading = false;
			int sendTo = 0;
	
			while (doneReading == false) {
				for (i=0; i<NUM_LINES; i++) {
					tweet = br.readLine();
					if (tweet != null) { 
						block[i] = tweet;
						tweetNum++;
					} else {
						doneReading = true;
						while (i < NUM_LINES) {
							block[i] = null;
							i++;
						}
						break;
					}
				}
			
			
				Tweets.sendBlock(sendTo, block, tweetNum);
				sendTo = (sendTo++) % (Tweets.NUM_THREADS);
			}

			br.close();	
								
		} catch (FileNotFoundException e) {
			System.out.println(System.getProperty("user.dir"));
			System.out.println("Caught IOException: " + e.getMessage());

		} catch (IOException e) {
			System.out.println("Caught IOException: " + e.getMessage());

		} finally {
			if (br != null) {
				try {
					br.close(); 
				} catch (IOException e) {
					System.out.println("Caught IOException: " + e.getMessage());
				}
			}
		}
		
		try {
			this.sleep(30);

		} catch (InterruptedException e) {
			System.out.println("Caught InterruptedException: " + e.getMessage());
		}

		readAll.set(true);

		while (workersFinished.get() < Tweets.NUM_THREADS) {
			continue;
		}
		
		Tweets.runLastThreads();			

    	}


}


class WorkerThread extends Thread {

	public static int numWorkers;
	public static int tweetNumber;
	public int index;
	public String[] tweets;
	public String[][] words;
	public int[] uniqueWordCount;
	public AtomicBoolean readyForData;
	
	public WorkerThread(int num) {
		index = num;
		numWorkers++;
		readyForData = new AtomicBoolean(true);
		words = new String[MasterThread.NUM_LINES][];
		uniqueWordCount = new int[MasterThread.NUM_LINES];
	}

	public void processTweets() {
		int i;
		Set<String> uniqueWords;
		for (i=0; i<MasterThread.NUM_LINES; i++) {
			if (tweets[i] != null) {
				words[i] = tweets[i].split("\\s+");
				uniqueWords = new HashSet<String>(Arrays.asList(words[i]));
				uniqueWordCount[i] = uniqueWords.size();	
			}

			else {
				while (i < MasterThread.NUM_LINES) {
					words[i] = null;
					i++;
				}

				break;
			}
		}
		
		Tweets.updateCounts(words, uniqueWordCount, tweetNumber);	

	}


	public void recvBlock(String[] block, int tweetNum) {
		while (readyForData.get() == false) {
			continue;
		}

		tweets = block;	
		tweetNumber = tweetNum; 
		readyForData.set(false);			
	}

	public void run() {
		boolean ready;
		while ((MasterThread.readAll.get()) == false) {
			ready = readyForData.get();
			if (ready == false) {
				processTweets();
				readyForData.set(true);
			}
			
		}

		MasterThread.workersFinished.incrementAndGet();	

	}

}


class MedianThread extends Thread {
	
	public HashMap<Integer,Integer[]> counts;
	public ArrayList<Integer> tweets;
	public int tweetNum;
	public static String file3;

	public MedianThread() {
		counts = Tweets.wordCounts;	
		tweets = new ArrayList<Integer>();
		tweetNum = 0;
	}

	public String calcMedian(int numTweets) {
		int middle1 = numTweets/2;
		int middle2 = middle1-1;
		double med;
		String roundedMed;

		if ((numTweets % 2) == 1) {
			med = (double) (tweets.get(middle1));
			roundedMed = String.format("%.2f", med);
			return roundedMed;
		}

		else {
			med = ((double) tweets.get(middle1) + (double) tweets.get(middle2))/2;
			roundedMed = String.format("%.2f", med);
			return roundedMed;
		}		

	}

	public void run() {
		int size = counts.size();
		Integer[] uniqueWords;
		String median;
		boolean done = false;

		FileWriter file3w = null;
		BufferedWriter file3bw = null;

		try {

			file3w = new FileWriter(file3);
			file3bw = new BufferedWriter(file3w);
			
			for (int i=0; i<size; i++) {
				uniqueWords = counts.get(new Integer(i+1));
				int numWords = uniqueWords.length;

				for (int j=0; j<numWords; j++) {
					if (uniqueWords[j] == 0) {
						done = true;
						break;
					}
					tweetNum++;
					tweets.add(uniqueWords[j]);
					Collections.sort(tweets);
					median = calcMedian(tweetNum);
					file3bw.write(median);
					file3bw.newLine();
				}

				if (done == true) {
					break;
				}	
			}
	
		} catch (IOException e) {
			System.out.println("Caught IOException: " + e.getMessage());
		} finally {
			if(file3bw != null){
                		try {
					file3bw.flush();
                    			file3bw.close();
                		} catch (IOException e) {
                    			System.out.println("Caught IOException: " + e.getMessage());
                		}
            		}

		}

	}



}


class CountThread extends Thread {
	
	public TreeMap<String, AtomicInteger> tweets;
	public static String file2;

	public CountThread() {
		tweets = Tweets.tweetMap;		
	}

	public void run() {
		Iterator iter = tweets.entrySet().iterator();
		String key;
		AtomicInteger value;
		String result;

		FileWriter file2w = null;
		BufferedWriter file2bw = null;

		try {	
			file2w = new FileWriter(file2);
			file2bw = new BufferedWriter(file2w); 	

			while (iter.hasNext()) {
        			Map.Entry tweet = (Map.Entry) iter.next();
				key = (String) tweet.getKey();
				value = (AtomicInteger) tweet.getValue();
				result = String.format("%-30s%5d", key, value.get());
				file2bw.write(result);
				file2bw.newLine();
    			}
			
	
		} catch (IOException e) {
			System.out.println("Caught IOException: " + e.getMessage());
		}
		finally {
			if(file2bw != null){
                		try {
					file2bw.flush();
                    			file2bw.close();
                		} catch (IOException e) {
                    			System.out.println("Caught IOException: " + e.getMessage());
                		}
            		}

		}

		
	}

}


public class Tweets {

	public static final int NUM_THREADS = 8;
	public static WorkerThread[] workers;
	public static MedianThread median;
	public static CountThread sortedWordCounts;
	public static TreeMap<String, AtomicInteger> tweetMap;
	public static HashMap<Integer, Integer[]> wordCounts;
	public static int tweetIndex = 0;

	public static void runLastThreads() {
		median = new MedianThread();
		sortedWordCounts = new CountThread();
		median.start();
		sortedWordCounts.start();
	}

	public static void sendBlock(int threadNum, String[] block, int tweetNumber) {
		workers[threadNum].recvBlock(block, tweetNumber);			
	}
	
	public static synchronized void updateCounts(String[][] words, int[] counts, int tweetNum) {
		int len;
		int countKey = tweetNum/(MasterThread.NUM_LINES);
		String key;

		if ((tweetNum % (MasterThread.NUM_LINES)) != 0) {
			countKey++;
		}

		Integer[] countWords = new Integer[MasterThread.NUM_LINES];
		for (int i=0; i<MasterThread.NUM_LINES; i++) {
			countWords[i] = Integer.valueOf(counts[i]);
		}
		wordCounts.put(new Integer(countKey), countWords);

		for (int i=0; i<MasterThread.NUM_LINES; i++) {
			if (words[i] == null) {
				break;
			} else {
				len = words[i].length;
			}

			for (int j=0; j<len; j++) {
				key = words[i][j];
				if (tweetMap.containsKey(key)) {
					tweetMap.get(key).incrementAndGet();
				} else {
					tweetMap.put(key, new AtomicInteger(1));
				}
			}		
						
				
		}

	} 

	public static void main(String[] args) {

		tweetMap = new TreeMap<String,AtomicInteger>();
		wordCounts = new HashMap<Integer, Integer[]>();

		MasterThread.file = args[0];
		CountThread.file2 = args[1];
		MedianThread.file3 = args[2];
		MasterThread t1 = new MasterThread();

		workers = new WorkerThread[NUM_THREADS];
		for (int i=0; i<NUM_THREADS; i++) {
			workers[i] = new WorkerThread(i);
			workers[i].start();
		}

		t1.start();	

	}

}
