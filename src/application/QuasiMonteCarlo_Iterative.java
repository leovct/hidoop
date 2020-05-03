package application;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * QuasiMonteCarlo iterative algorithm (should specify nbr of points generated in arg)
 *
 */
public class QuasiMonteCarlo_Iterative {

	private static long pointsGeneratedPerMap = 1000000;
	private static BufferedWriter writer;
	
	public static void main(String[] args) {
		long t1 = System.currentTimeMillis();
		
		// GENERATE POINTS
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
				System.out.println("QuasiMonteCarlo: Generated " + i + " points");
			}
		}
		
		// WRITE RESULTS
		try {
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("quasi-monte-carlo-iterative-res")));
			writer.write("insideCircle"+"<->"+inside);
			writer.newLine();
			writer.write("insideSquare"+"<->"+pointsGeneratedPerMap);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
		// DISPLAY RESULTS AND COMPUTATION TIME
		long t2 = System.currentTimeMillis();
		System.out.println("Computation time for the QuasiMonteCarlo iterative algorithm: "+(t2-t1)+"ms");
        System.out.println("PI approximate value: " + (double)4*inside/pointsGeneratedPerMap);
	}
	
}
