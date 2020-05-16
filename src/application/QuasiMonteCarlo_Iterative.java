package application;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * QuasiMonteCarlo iterative algorithm which approximate the value of PI.
 * Note: The precision depends on the number of points generated (10^6 is the default value).
 */
public class QuasiMonteCarlo_Iterative {

	private static long pointsGeneratedPerMap = 1_000_000L;
	private static BufferedWriter writer;
	
	public static void main(String[] args) {
		long t1 = System.currentTimeMillis();
		
		// Generate points
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
		
		// Write results
		try {
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("quasi-monte-carlo-iterative-res")));
			writer.write("insideCircle"+"<->"+inside);
			writer.newLine();
			writer.write("insideSquare"+"<->"+pointsGeneratedPerMap);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
		// Display results and computation time
		long t2 = System.currentTimeMillis();
		System.out.println("[QUASIMONTECARLO] Computation time for the Iterative version: " + (t2-t1) + "ms");
        System.out.println("[QUASIMONTECARLO] PI approximate value: " + (double)4*inside/pointsGeneratedPerMap);
	}
	
}
