package application;
 
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * WordCount iterative algorithm.
 * It calculates the number of times each word appears in a file.
 */
public class WordCount_Iterative {

	public static void main(String[] args) {
		try {
            long t1 = System.currentTimeMillis();

			Map<String,Integer> hm = new HashMap<>();
			LineNumberReader lnr = new LineNumberReader(new InputStreamReader(new FileInputStream(args[0])));
			while (true) {
				String l = lnr.readLine();
				if (l == null) {
					break;
				}
				StringTokenizer st = new StringTokenizer(l);
				while (st.hasMoreTokens()) {
					String tok = st.nextToken();
					if (hm.containsKey(tok)) {
						hm.put(tok, hm.get(tok).intValue()+1);
					} else {
						hm.put(tok, 1);
					}
				}
			}
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("wordcount-iterative-res")));
			for (String k : hm.keySet()) {
				writer.write(k+"<->"+hm.get(k).toString());
				writer.newLine();
			}
			writer.close();
			lnr.close();
            long t2 = System.currentTimeMillis();
            System.out.println("[WORDCOUNT] Computation time for the Iterative version: " + (t2-t1) + "ms");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
