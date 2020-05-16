package application;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import config.SettingsManager;
import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import formats.KVFormat;
import hdfs.HdfsClient;
import map.MapReduce;
import ordo.JobClient;

/**
 * Google's page ranking MapReduce algorithm (simplified version).
 * It is used to rank web pages in the popular search engine and was invented by Larry Page.
 * 	- Map: Emit the slice of the node page rank to the neighbors
 * 	- Reduce: Compute the next page rank with the result of the Map operations and emit
 * 		the new page ranks of the nodes of the network.
 */
public class PageRank_MapReduce implements MapReduce {
	
	private static final long serialVersionUID = 1L;
	
	@Override
	public void map(FormatReader reader, FormatWriter writer) {
		KV kv;
		
		// Emit the slice of page rank given to the neighbors in order to compute the next page ranks
		while ((kv = reader.read()) != null) {
			String[] split = kv.v.split("<->|,");
			double newPageRank = Double.parseDouble(split[split.length - 1]) / (split.length - 2);
			for(int i = 1; i < split.length - 1; i++) {
				writer.write(new KV((String) split[i], Double.toString(newPageRank)));
			}
		}
	}

	@Override
	public void reduce(FormatReader reader, FormatWriter writer) {
		KV kv;
		Map<String,Double> hm = new HashMap<>();
		
		// Compute the current page rank sum for every Node
		while ((kv = reader.read()) != null) {
			if (hm.containsKey(kv.k)) {
				hm.put(kv.k, hm.get(kv.k) + Double.parseDouble(kv.v));
			} else {
				hm.put(kv.k, Double.parseDouble(kv.v));
			}	
		}
		
		// Compute the new page rank using the formula for every Node
		double dumpingFactor = 0.85;
		for(String key : hm.keySet()) {
			hm.put(key, (1 - dumpingFactor)/hm.size() + dumpingFactor * hm.get(key));
		}
		
		// Emit the new page ranks for every Node
		for (String key : hm.keySet()) {
			writer.write(new KV(key, hm.get(key).toString()));
		}
	}
	
	/**
	 * Return the number of pages (nodes) in the network.
	 * @param network
	 * @return size of the network
	 */
	public static int getNetworkSize(String network) {
		int size = 0;
		KVFormat input = new KVFormat(network);
		input.open(Format.OpenMode.R);
		KV read;
		while((read = input.read()) != null) {
			size++;
		}
		return size;
	}
	
	/**
	 * Display the page (node) that is statistically more likely to be visited to the page that is statistically less likely to be visited.
	 * @param networkSize the number of pages in the network
	 */
	public static void showResults(int networkSize, int nbrIterations) {
		Map<Double, String> hm = new HashMap<>();
		double[] values = new double[networkSize];
		
		// Read the result of the last Reduce operation and store it in a Hashmap
		KVFormat result = new KVFormat(SettingsManager.RESULTS_FOLDER + SettingsManager.TAG_RESULT + "pagerank-iter" + (nbrIterations-1));
		result.open(Format.OpenMode.R);
		KV read;
		int index = 0;
		while((read = result.read()) != null) {
			hm.put(Double.parseDouble(read.v), read.k);
			values[index] = Double.parseDouble(read.v);
			index++;
		}
		
		// Sort the values
		Arrays.sort(values);
		
		// Display the results
		System.out.println("[PAGE RANK] Nodes ranking:");
		index = 1;
		for(int i = values.length - 1; i >= 0; i--) {
			System.out.println("[PAGE RANK] " + index + ". " + hm.get(values[i]));
			index++;
		}
	}
	
	/**
	 * Delete all the files used during the iterations of the page ranking algorithm.
	 * @param nbrIteration number of iterations
	 */
	public static void deleteFiles(int nbrIterations) {
		for(int i = 0; i < nbrIterations; i++) {
			// Delete files on HDFS
	        HdfsClient.HdfsDelete("pagerank-iter"+i);
	       	
	        // Delete files in the SettingsManager.RESULTS_FOLDER
	        // File results/pagerank-iter
	        if (new File(SettingsManager.RESULTS_FOLDER + "pagerank-iter"+i).delete()) {
	        	System.out.println("[PAGE RANK] " + SettingsManager.RESULTS_FOLDER + "pagerank-iter"+i + " deleted");
	        } else {
	    	   System.out.println("[PAGE RANK] Issue with " + SettingsManager.RESULTS_FOLDER + "pagerank-iter"+i);
	        }
	        	
	        if (i < nbrIterations -1) {
	    	// File results/map-pagerank-iter
	        	if(new File(SettingsManager.RESULTS_FOLDER + SettingsManager.TAG_MAP + "pagerank-iter"+i).delete()) {
	           		System.out.println("[PAGE RANK] " + SettingsManager.RESULTS_FOLDER + SettingsManager.TAG_MAP + "pagerank-iter"+i + " deleted");
	           	} else {
	           		System.out.println("[PAGE RANK] Issue with " + SettingsManager.RESULTS_FOLDER + SettingsManager.TAG_MAP + "pagerank-iter"+i);
	           	}
	            	
	           	// File results/resf-pagerank-iter
	            if (new File(SettingsManager.RESULTS_FOLDER + SettingsManager.TAG_RESULT + "pagerank-iter"+i).delete()) {
	              	System.out.println("[PAGE RANK] " + SettingsManager.RESULTS_FOLDER + SettingsManager.TAG_RESULT + "pagerank-iter"+i + " deleted");
	            } else {
	           		System.out.println("[PAGE RANK] Issue with " + SettingsManager.RESULTS_FOLDER + SettingsManager.TAG_RESULT + "pagerank-iter"+i);
	          	}
	        }
	    }
	}
	
	public static void main(String args[]) {
		// Handle wrong use cases
		if(args.length < 2) {
			System.out.println("Wrong arguments.");
			System.exit(0);
		} else if (args.length == 2) {
			if (Integer.parseInt(args[1]) == 0) {
				System.out.println("Number of iteration has to be positive.");
				System.exit(0);
			}
		}
		
		// Start the timer
        long time = System.currentTimeMillis();
		
        // Get the size of the network		
		int size = getNetworkSize(args[0]);
		System.out.println("[PAGE RANK] Size of the network: " + size + " node(s)");
		
		// Iterate over the network to compute the page ranks
		for(int i = 0; i < Integer.parseInt(args[1]); i++) {
			System.out.println("[PAGE RANK] ITERATION " + i);
			
			// Get the page rank values from the last reduce (or initialize them at 1/size)
			double[] pageRanks = new double[size];
			if (i == 0) {
				// if it is the first iteration, store 1/size values
				for(int j = 0; j < size; j++) {
					pageRanks[j] = (double) 1/size;
				}
			} else {
				// else, store the result of the last Reduce
				KVFormat reduce = new KVFormat(SettingsManager.RESULTS_FOLDER + SettingsManager.TAG_RESULT + "pagerank-iter"+(i-1));
				reduce.open(Format.OpenMode.R);
				KV kv;
				int j = 0;
				while((kv = reduce.read()) != null) {
					pageRanks[j] = Double.parseDouble(kv.v);
					j++;
				}
			}
			
			// Update the page rank values
			KVFormat network = new KVFormat(args[0]);
			network.open(Format.OpenMode.R);
			KVFormat nextIteration = new KVFormat(SettingsManager.RESULTS_FOLDER + "pagerank-iter"+i);
			nextIteration.open(Format.OpenMode.W);
			for(int k = 0; k < size; k++) {
				KV networkRow = network.read();
				nextIteration.write(new KV(networkRow.k, networkRow.v + "," + pageRanks[k]));
			}
			
			// Write the updated network on HDFS (not very efficient...)
			HdfsClient.HdfsWrite(Format.Type.LINE, SettingsManager.RESULTS_FOLDER + "pagerank-iter"+i, 1);
			
			// Start a new job (Map/Reduce) for this iteration
			JobClient job = new JobClient(Format.Type.LINE, SettingsManager.RESULTS_FOLDER + "pagerank-iter" + i);
			job.startJob(new PageRank_MapReduce());
		}
		
		// Show results and computation time (not representative on this application because of HdfsWrite...)
		time = System.currentTimeMillis() - time;
        System.out.println("[PAGE RANK] Computation time: " + time + "ms");
        showResults(size, Integer.parseInt(args[1]));
        
        // Delete files
        System.out.println("[PAGE RANK] Clean results folder");
        deleteFiles(Integer.parseInt(args[1]));
        
        System.exit(0);        
	}

}
