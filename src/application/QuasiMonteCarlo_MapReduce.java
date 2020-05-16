package application;

import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.JobClient;

/**
 * QuasiMonteCarlo MapReduce algorithm which approximate the value of PI.
 * Note: The precision depends on the number of points generated.
 * 	- Map: Generate 10^6 points (default value) in a unit square.
 * 	- Reduce: Count the points inside and outside the inscribed circle in the unit square.
 */
public class QuasiMonteCarlo_MapReduce implements MapReduce {
	
	private static final long serialVersionUID = 1L;
	private static long insideCircle;
	private static long generated;
	private static long pointsGeneratedPerMap = 1_000_000L;
	
	@Override
	public void map(FormatReader reader, FormatWriter writer) {
		long inside = 0;
		
		for(long i = 1; i <= pointsGeneratedPerMap; i++) {
			// Produce a point using random (should use halton sequence)
			double points[] = {Math.random(), Math.random()};
			
			// Count points inside and outside the inscribed circle in the unit square
			double x = points[0];
			double y = points[1];
			
			if (x*x + y*y <= 1) {
				inside++;
			}
			
			// Display the msg when i/5th points have been generated
			if (i % (pointsGeneratedPerMap/5) == 0) {
				System.out.println("[QUASIMONTECARLO] Generated " + i + " points");
			}
		}

		// Write results inside a file
		writer.write(new KV("insideCircle", Long.toString(inside)));
		writer.write(new KV("insideSquare", Long.toString(pointsGeneratedPerMap)));
	}

	@Override
	public void reduce(FormatReader reader, FormatWriter writer) {
		// Count points inside and outside the inscribed circle in the unit square produced after each map
		KV kv;
		insideCircle = 0;
		generated = 0;
		while ((kv = reader.read())	!= null) {
			if (kv.k.equalsIgnoreCase("insideCircle")) {
				insideCircle += Long.parseLong(kv.v);
			} else {
				generated += Long.parseLong(kv.v);
			}
		}
		
		// Write final results inside a file
		writer.write(new KV("insideCircle", Long.toString(insideCircle)));
		writer.write(new KV("insideSquare", Long.toString(generated)));
	}
	
	public static void main(String args[]) {
		JobClient j = new JobClient("quasi-monte-carlo");
        long t1 = System.currentTimeMillis();
		j.startJob(new QuasiMonteCarlo_MapReduce());
		long t2 = System.currentTimeMillis();
        System.out.println("[QUASIMONTECARLO] Computation time for the MapReduce version: " + (t2-t1) + "ms");
        System.out.println("[QUASIMONTECARLO] PI approximate value: " + (double)4*insideCircle/generated);
        System.exit(0);
	}

}
