/*
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

package jaligner.test;

import jaligner.Alignment;
import jaligner.Sequence;
import jaligner.SmithWatermanGotoh;
import jaligner.formats.Format;
import jaligner.formats.Pair;
import jaligner.matrix.Matrix;
import jaligner.matrix.MatrixLoader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Testing the scores of the alignments of {@link SmithWatermanGotoh}.
 *
 * @author Bram Minnaert
 */

public class SmithWatermanGotohTester {

    public static void main ( String[] args ) {

	int    numberOfTests = Integer.parseInt(args[0]);
	int    sequencesSize = Integer.parseInt(args[1]);
	Random random        = new Random();
	Format format        = new Pair();

	try {
	    List<String> matrices = new ArrayList<String>();
	    for ( Iterator<String> i = MatrixLoader.list().iterator(); i.hasNext(); ) {
		matrices.add(i.next());
	    }
	    int countOfMatrices = matrices.size();

	    int i = 1;
	    while ( i <= numberOfTests ) {

		System.gc();

		String s1 = RandomSequenceGenerator.generate(sequencesSize);
		String s2 = RandomSequenceGenerator.generate(sequencesSize);

		int o = random.nextInt(50);
		int e = random.nextInt(10);

		if ( s1.length() > 0 && s2.length() > 0 && o >= e ) {

		    Matrix matrix = (Matrix) MatrixLoader.load((String) matrices.get(random.nextInt(countOfMatrices)));
		    Sequence seq1 = new Sequence(s1);
		    Sequence seq2 = new Sequence(s2);

		    Alignment alignment1 = SmithWatermanGotoh.align(seq1, seq2, matrix, o, e);

		    if ( !alignment1.checkScore() ) {
			System.err.println("Invalid alignment found:");
			System.err.println("Sequence 1 = " + s1);
			System.err.println("Sequence 2 = " + s2);
			System.err.println(format.format(alignment1));
			System.err.println(alignment1.getSummary());
			System.err.println("The score of the alignment above is: " + alignment1.calculateScore());
			System.exit(1);
		    }

		    Alignment alignment2 = SmithWatermanGotoh.align(seq2, seq1, matrix, o, e);
		    if ( !alignment1.checkScore() ) {
			System.err.println("Invalid alignment found:");
			System.err.println("Sequence 1 = " + s2);
			System.err.println("Sequence 2 = " + s1);
			System.err.println(format.format(alignment2));
			System.err.println(alignment2.getSummary());
			System.err.println("The score of the alignment above is: " + alignment2.calculateScore());
			System.exit(1);
		    }

		    if ( alignment1.getScore() != alignment2.getScore() ) {
			System.err.println("Not symmetric alignment:");

			System.err.println("Alignment #1: ");
			System.err.println("Sequence 1 = " + s1);
			System.err.println("Sequence 2 = " + s2);
			System.err.println(format.format(alignment1));
			System.err.println(alignment1.getSummary());

			System.err.println();

			System.err.println("Alignment #2: ");
			System.err.println("Sequence 1 = " + s2);
			System.err.println("Sequence 2 = " + s1);
			System.err.println(format.format(alignment2));
			System.err.println(alignment2.getSummary());

			System.exit(1);
		    }
		}
		System.out.println("Processed " + i + "/" + numberOfTests);
		i++;
	    }
	} catch ( Exception e ) {
	    e.printStackTrace();
	    System.exit(1);
	}
    }
}