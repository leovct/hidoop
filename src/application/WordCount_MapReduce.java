package application;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import map.MapReduce;
import ordo.JobClient;
import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;

/**
 * WordCount MapReduce algorithm.
 * It calculates the number of times each word appears in a file.
 * 	- Map: Count the number of times each word appears in a chunk.
 * 	- Reduce: Gather the results of the Map operations.
 */
public class WordCount_MapReduce implements MapReduce {
	private static final long serialVersionUID = 1L;

	@Override
	public void map(FormatReader reader, FormatWriter writer) {
		Map<String,Integer> hm = new HashMap<>();
		KV kv;
		while ((kv = reader.read()) != null) {
			StringTokenizer st = new StringTokenizer(kv.v);
			while (st.hasMoreTokens()) {
				String tok = st.nextToken();
				if (hm.containsKey(tok)) hm.put(tok, hm.get(tok).intValue()+1);
				else hm.put(tok, 1);
			}
		}
		for (String k : hm.keySet()) {
			writer.write(new KV(k,hm.get(k).toString()));
		}
	}
	
	@Override
	public void reduce(FormatReader reader, FormatWriter writer) {
		Map<String,Integer> hm = new HashMap<>();
		KV kv;
		while ((kv = reader.read()) != null) {
			if (hm.containsKey(kv.k)) hm.put(kv.k, hm.get(kv.k)+Integer.parseInt(kv.v));
			else hm.put(kv.k, Integer.parseInt(kv.v));
		}
		for (String k : hm.keySet()) {
			writer.write(new KV(k,hm.get(k).toString()));
		}
	}
	
	public static void main(String args[]) {
		JobClient j = new JobClient(Format.Type.LINE,args[0]);
        long time = System.currentTimeMillis();
		j.startJob(new WordCount_MapReduce());
		time = System.currentTimeMillis() - time;
		System.out.println("[WORDCOUNT] Computation time for the MapReduce version: " + time + "ms");
        System.exit(0);
		}
}
