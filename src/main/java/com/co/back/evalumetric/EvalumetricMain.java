package com.co.back.evalumetric;

import com.co.back.evalumetric.evaluation.loss.Loss;
import com.co.back.evalumetric.evaluation.loss.impl.PairBased;
import com.co.back.evalumetric.evaluation.loss.impl.PrecisionAtK;
import com.co.back.evalumetric.evaluation.loss.impl.Silhouette;
import com.co.back.evalumetric.evaluation.loss.impl.TripletBased;
import com.co.back.evalumetric.evaluation.metrics.Metric;
import com.co.back.evalumetric.evaluation.metrics.impl.AlignmentNW;
import com.co.back.evalumetric.evaluation.metrics.impl.AlignmentNWf1;
import com.co.back.evalumetric.evaluation.metrics.impl.EditGeneric;
import com.co.back.evalumetric.evaluation.metrics.impl.EditLevenshtein;
import com.co.back.evalumetric.evaluation.metrics.impl.EditLevenshteinNorm;
import com.co.back.evalumetric.evaluation.metrics.impl.EventuallyFollowsDelias;
import com.co.back.evalumetric.evaluation.metrics.impl.MRCosine;
import com.co.back.evalumetric.evaluation.metrics.impl.MREuclidean;
import com.co.back.evalumetric.evaluation.metrics.impl.NGramCosine;
import com.co.back.evalumetric.evaluation.metrics.impl.NGramEuclidean;
import com.co.back.evalumetric.evaluation.metrics.impl.NSMRCosine;
import com.co.back.evalumetric.evaluation.metrics.impl.NSMREuclidean;
import com.co.back.evalumetric.evaluation.metrics.impl.SMRCosine;
import com.co.back.evalumetric.evaluation.metrics.impl.SMREuclidean;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EvalumetricMain {

    public static final  String                          FLAG_K                  = "k";
    public static final  String                          FLAG_LONG_DEDUPLICATE   = "--deduplicate";
    private static final String                          FLAG_LONG_INPUT         = "--input";
    private static final String                          FLAG_LONG_MERGE         = "--merge";
    private static final String                          FLAG_THREADS            = "--threads";
    private static       boolean                         deduplicate             = false;
    private static final Set<Class<? extends Loss>>      lossFunctionClasses     = new HashSet<>(Arrays.asList(PairBased.class, TripletBased.class, PrecisionAtK.class, Silhouette.class));
    private static final Set<Class<? extends Metric<?>>> similarityMetricClasses = new HashSet<>(Arrays.asList(NGramCosine.class, NGramEuclidean.class, EditLevenshtein.class, EditLevenshteinNorm.class, EditGeneric.class, EventuallyFollowsDelias.class, MRCosine.class, SMRCosine.class, NSMRCosine.class, MREuclidean.class, SMREuclidean.class, NSMREuclidean.class, AlignmentNW.class, AlignmentNWf1.class));
    private static       boolean                         merge                   = false;
    private static       int                             threads                 = 1;

    public static void main ( String[] args ) throws InstantiationException, IllegalAccessException, IOException {

	final List<String>   inputFilePaths    = new ArrayList<>();
	final Set<Loss>      lossFunctions     = new LinkedHashSet<>();
	final Set<Metric<?>> similarityMetrics = new LinkedHashSet<>();

	processArgs(args, inputFilePaths, lossFunctions, similarityMetrics);

	Evalumetric e = new Evalumetric(inputFilePaths, lossFunctions, similarityMetrics, merge, deduplicate);

	System.out.println("threads: " + threads);

	final ExecutorService es = Executors.newFixedThreadPool(threads);

	try {
	    e.run2(es);
	} catch ( IOException | SQLException ex ) {
	    ex.printStackTrace();
	}

	es.shutdown();
    }

    private static void printHelp () throws InstantiationException, IllegalAccessException {

	System.out.printf("%-40s%-40s" + System.lineSeparator(), "-h/--help", "Print this prompt");
	System.out.printf("%-40s%-40s" + System.lineSeparator(), "--input=<FILES>", "Input files (comma-separated)");
	System.out.println();
	System.out.printf("%-80s" + System.lineSeparator(), "LOSS FUNCTIONS: parameter args should be comma-separated");
	for ( Class<? extends Loss> c : lossFunctionClasses ) {
	    System.out.printf("%-40s%-40s" + System.lineSeparator(), "--" + c.getSimpleName() + ( c.newInstance().argsDescription() != null ? "=<ARGS>" : "" ), c.newInstance().argsDescription());
	}
	System.out.println();
	System.out.printf("%-80s" + System.lineSeparator(), "SIMILARITY METRICS: parameter args should be comma-separated");
	for ( Class<? extends Metric<?>> c : similarityMetricClasses ) {
	    System.out.printf("%-40s%-40s" + System.lineSeparator(), "--" + c.getSimpleName() + ( c.newInstance().argsDescription() != null ? "=<ARGS>" : "" ), c.newInstance().argsDescription());
	}
    }

    private static void processArgs ( String[] args, List<String> inputFilePaths, Set<Loss> lossFunctions, Set<Metric<?>> similarityMetrics ) throws InstantiationException, IllegalAccessException {

	if ( Arrays.asList(args).contains("-h") || Arrays.asList(args).contains("--help") ) {

	    printHelp();

	} else {

	    for ( String arg : args ) {

		String argLower = arg.toLowerCase();
		System.out.println(argLower);
		if ( argLower.equalsIgnoreCase(FLAG_LONG_DEDUPLICATE) ) {
		    deduplicate = true;
		} else if ( argLower.startsWith(FLAG_LONG_INPUT + "=") ) {
		    String[] fileArr = arg.split("=");
		    if ( fileArr.length == 2 ) {
			inputFilePaths.addAll(Arrays.asList(fileArr[1].split(",")));
		    }
		} else if ( argLower.equalsIgnoreCase(FLAG_LONG_MERGE) ) {
		    merge = true;
		} else if ( argLower.startsWith(FLAG_THREADS) ) {
		    String[] threadsArr = arg.split("=");
		    threads = Integer.parseInt(threadsArr[1]);
		} else {
		    for ( Class<? extends Loss> c : lossFunctionClasses ) {
			String a = argLower.contains(":") ? argLower.split(":")[0] : argLower;
			if ( a.startsWith("--" + c.getSimpleName().toLowerCase()) ) {
			    Loss l = c.newInstance();
			    if ( argLower.contains(":") ) {
				l.processArgs(argLower.split(":")[1].split(","));
			    }
			    lossFunctions.add(l);
			}
		    }
		    for ( Class<? extends Metric<?>> c : similarityMetricClasses ) {
			String a = argLower.contains(":") ? argLower.split(":")[0] : argLower;
			if ( a.equals("--" + c.getSimpleName().toLowerCase()) ) {
			    Metric<?> m = c.newInstance();
			    if ( argLower.contains(":") ) {
				m.processArgs(argLower.split(":")[1].split(","));
			    }
			    similarityMetrics.add(m);
			}
		    }
		}
	    }
	}
    }

}
