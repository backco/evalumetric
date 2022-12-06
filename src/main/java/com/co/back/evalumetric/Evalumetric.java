package com.co.back.evalumetric;

import com.co.back.evalumetric.data.DataImport;
import com.co.back.evalumetric.data.NullOutputStream;
import com.co.back.evalumetric.data.RelationalLog;
import com.co.back.evalumetric.data.RelationalLogXES;
import com.co.back.evalumetric.data.RelationalTrace;
import com.co.back.evalumetric.embeddings.Embeddings;
import com.co.back.evalumetric.evaluation.loss.Loss;
import com.co.back.evalumetric.evaluation.metrics.Metric;
import com.co.back.evalumetric.evaluation.metrics.impl.EditGeneric;
import com.co.back.flexes.FleXES;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public class Evalumetric implements Runnable {

    private final boolean        deduplicate;
    private final List<String>   inputFilePaths;
    private final Set<Loss>      lossFunctions;
    private final boolean        merge;
    private final Set<Metric<?>> similarityMetrics;
    private final String         delimiter = "&";
    private double requestedSampleRatio = 0.01;
    //private double requestedSampleRatio = 0.001;
    //private double requestedSampleRatio = 1.0;

    public Evalumetric ( List<String> inputFilePaths, Set<Loss> lossFunctions, Set<Metric<?>> similarityMetrics, boolean merge, boolean deduplicate ) {

	this.deduplicate       = deduplicate;
	this.inputFilePaths    = inputFilePaths;
	this.lossFunctions     = lossFunctions;
	this.merge             = merge;
	this.similarityMetrics = similarityMetrics;
    }

    private int longestClassNameLength ( List<String> inputFilePaths ) throws IOException {

	int max = "CLASS".length();

	for ( String f : inputFilePaths ) {

	    DataImport.MetaData md = DataImport.getMetaData(f);

	    for ( String c : md.getTraceClassName() ) {

		max = c.length() > max ? c.length() : max;
	    }
	}

	return max;
    }

    private int longestFileNameLength ( List<String> inputFilePaths ) {

	int max = "DATASET".length();

	for ( String f : inputFilePaths ) {
	    max = f.length() > max ? f.length() : max;
	}

	return max;
    }

    private int longestMetricNameLength ( Set<Metric<?>> similarityMetrics ) {

	int max = "METRIC".length();

	for ( Metric<?> m : similarityMetrics ) {
	    max = m.getClass().getSimpleName().length() > max ? m.getClass().getSimpleName().length() : max;
	}

	return max;
    }

    @Override
    public void run () {

    }

    //@Override
    public void run2 ( ExecutorService es ) throws IOException, SQLException {

	int fileMax   = longestFileNameLength(inputFilePaths);
	int classMax  = merge ? "sublog".length() : longestClassNameLength(inputFilePaths);
	int metricMax = longestMetricNameLength(similarityMetrics);

	StringBuilder header = new StringBuilder();

	header.append(String.format("%-" + fileMax + "s " + delimiter, "DATASET"));
	header.append(String.format(" %-" + classMax + "s " + delimiter, "CLASS"));
	header.append(String.format(" %-" + metricMax + "s ", "METRIC"));

	for ( Loss l : lossFunctions ) {
	    header.append(delimiter + " ").append(l.getClass().getSimpleName()).append(" ");
	}

	System.out.println("deduplicate? " + deduplicate);
	System.out.println("merge? " + merge);

	System.out.println(header);

	List<Pair<RelationalLog, String>> logsWithClasses = new ArrayList<>();

	if ( merge ) {

	    NavigableMap<String, XLog> logs = new TreeMap<>();

	    for ( String f : inputFilePaths ) {

		PrintStream out = System.out;
		System.setOut(new PrintStream(new NullOutputStream()));
		logs.put(f, FleXES.loadEventLog(f));
		System.setOut(out);
	    }
	    String              filename = String.join(".", inputFilePaths);
	    XLog                merged   = FleXES.merge(logs);
	    DataImport.MetaData md       = DataImport.getMetaData(filename);
	    RelationalLog       rLog     = DataImport.importXES(merged, "sublog", md.getEventClassifier());
	    rLog.name            = filename;
	    rLog.eventClassifier = md.getEventClassifier();
	    logsWithClasses.add(new ImmutablePair<>(rLog, "sublog"));

	} else {

	    for ( String f : inputFilePaths ) {

		//NavigableMap<Integer, Integer> hist = FleXES.traceLengthHistogram(FleXES.loadXES(f));
		//hist.navigableKeySet().forEach(k -> System.out.printf("%6d: %6d" + System.lineSeparator(), k, hist.get(k)));
		//System.exit(1);

		DataImport.MetaData md         = DataImport.getMetaData(f);
		List<String>        classNames = md.getTraceClassName();

		for ( String c : classNames ) {
		    System.out.println("CLASS: " + c);
		    RelationalLog rLog = DataImport.importXES(f, c, md.getEventClassifier());
		    rLog.eventClassifier = md.getEventClassifier();
		    rLog.name            = f;
		    logsWithClasses.add(new ImmutablePair<>(rLog, c));
		}
	    }
	}

	List<String> dummyInserts = new ArrayList<>();
	FileWriter   output       = new FileWriter("output.sql");

	if (deduplicate) {
	    logsWithClasses = deduplicate(logsWithClasses);
	}

	int countLog = 0;

	for ( Pair<RelationalLog, String> e : logsWithClasses ) {

	    countLog++;

	    System.out.println("\\hline");

	    RelationalLog rLog = e.getLeft();
	    String        c    = e.getRight();

	    int countMetric = 0;

	    for ( Metric<?> m : similarityMetrics ) {

		countMetric++;

		if ( m instanceof EditGeneric ) {
		    ( (EditGeneric) m ).init(rLog);
		}

		System.out.printf("%-" + fileMax + "s " + delimiter + " %-" + classMax + "s " + delimiter + " %-" + metricMax + "s ", rLog.name, c, m.getClass().getSimpleName());

		int countLoss = 0;

		for ( Loss l : lossFunctions ) {

		    countLoss++;

		    final String msg = String.format("[log %d/%d, metric %d/%d, loss %d/%d]   ", countLog, logsWithClasses.size(), countMetric, similarityMetrics.size(), countLoss, lossFunctions.size());

		    System.out.println("preparing evaluation measure: " + l.getClass().getSimpleName());

		    // prepare on new log
		    if ( l.getLog() == null || !l.getLog().equals(rLog) || !l.isPrepared() ) {
			l.prepare(rLog);
		    }

		    l.setSampleRatio(requestedSampleRatio);

		    System.out.println("computing evaluation measure: " + l.getClass().getSimpleName());

		    double sampleRatio, loss;

		    try {
			loss        = l.accuracy(es, rLog, m, msg);
			sampleRatio = l.getSampleRatio();
		    } catch ( ExecutionException | InterruptedException ex ) {
			throw new RuntimeException(ex);
		    }

		    System.out.printf(delimiter + " %" + l.getClass().getSimpleName().length() + "s ", round(loss, 5));

		    System.out.println("db insert start");
		    String metricLabel= m.getClass().getSimpleName() + "(" + m.argsDescription() + ")";
		    //String sql = "REPLACE INTO `results` VALUES ('" + rLog.name + "','" + c + "','" + metricLabel + "','" + l + "'," + loss + "," + sampleRatio + ",CURRENT_TIMESTAMP());";

		    String sql = dbInsert(rLog.name, c, metricLabel, l.toString(), sampleRatio, loss);
		    dummyInserts.add(sql);
		    output.write(sql);

		    System.out.println("db insert done");
		}
		System.out.println(" \\\\");
	    }
	}

	dummyInserts.forEach(System.out::println);
	output.close();
    }

    static List<Pair<RelationalLog, String>> deduplicate ( List<Pair<RelationalLog, String>> logsWithClasses ) {

	List<Pair<RelationalLog, String>> result = new ArrayList<>();
	Map<String, Set<String>> flattenedLog = new HashMap<>();

	for ( Pair<RelationalLog,String> subLog : logsWithClasses) {

	    if (subLog.getLeft() instanceof RelationalLogXES) {
		Set<String> tracePatterns = flattenedLog.computeIfAbsent(subLog.getRight(), v -> new HashSet<>());
		RelationalLogXES l = (RelationalLogXES) subLog.getLeft();
		RelationalLogXES dedupedLog = new RelationalLogXES();
		result.add(new ImmutablePair<>(dedupedLog,subLog.getRight()));
		for (RelationalTrace<XEvent, XAttribute> t : l) {
		     String pattern = Embeddings.traceToString(t);
		     if (!tracePatterns.contains(pattern)) {
			 tracePatterns.add(pattern);
			 dedupedLog.add(t);
		     }
		}
	    } else {
		throw new IllegalArgumentException("Log must be of type RelationalLogXES");
	    }
	}

	return result;
    }

    public static double round ( double d, int p ) {

	double m = Math.pow(10, p);
	return Math.round(d * m) / m;
    }

    static String dbInsert ( String filename, String classLabel, String similarityMetric, String evaluationMeasure, double sampleRatio, double score ) {

	String dbUsername = "linroot";
	String dbPassword = "so2$yUGCSU85qoLr";

	String sql = "REPLACE INTO `results` VALUES ('" + filename + "','" + classLabel + "','" + similarityMetric + "','" + evaluationMeasure + "'," + score + "," + sampleRatio + ", CURRENT_TIMESTAMP())";
	System.out.println(sql);

	boolean success = false;

	while ( !success ) {

	    try ( Connection conn = DriverManager.getConnection("jdbc:mysql://lin-10885-6680-mysql-primary.servers.linodedb.net:3306/research", dbUsername, dbPassword); Statement stmt = conn.createStatement() ) {

		stmt.execute(sql);
		success = true;
	    } catch ( SQLException e ) {

		e.printStackTrace();
		try {
		    Thread.sleep(1000);
		} catch ( InterruptedException ex ) {
		    ex.printStackTrace();
		}
	    }
	}


	return sql;
    }
}
