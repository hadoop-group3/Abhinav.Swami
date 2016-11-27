package stockscreener;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Calendar;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Spark application to list the tickers for each sector.
 *
 * Uses NASDAQ company list file
 *
 * @author AbhinavSwami
 *
 */

public class ChallengeSparkScreener {

	public static void main(String[] args) throws IOException, URISyntaxException {

		if (args.length < 2) {
			System.err.println("Usage: <inputPath> <outputPath>");
			System.exit(1);
		}

		// setup job configuration and context
		SparkConf sparkConf = new SparkConf().setAppName("Stock Screener");
		sparkConf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		// setup input and output
		JavaRDD<String> inputPath = sc.textFile(args[0], 1);
		// notice, this takes the output path and adds a date and time
		String outputPath = args[1] + "_" + Calendar.getInstance().getTimeInMillis();

		// parse the input line into an array of words
		JavaRDD<String> words = inputPath.flatMap(new FlatMapFunction<String, String>() {
			private final static String recordRegex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
			private final static int sectorIndex = 5;
			private final static int capIndex = 3;
			private final static int tickerIndex = 0;

			@Override
			public Iterable<String> call(String s) throws Exception {
				String[] tokens = s.split(recordRegex, -1);
				String sectorStr = tokens[sectorIndex].replace("\"", "");
				String tickerStr = tokens[tickerIndex].replace("\"", "");
				String capStr = tokens[capIndex].replace("\"", "");

				String str = sectorStr + "," + tickerStr + "," + capStr;
				return Arrays.asList(str);
			}
		});

		// Filter out unnecessary data
		JavaRDD<String> filters = words.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				String[] tokens = s.split(",");
				// Ignore tickers with market cap as "n/a" or having "B" and
				// ignore the header line
				if (tokens[2].contains("B") || tokens[2].contains("n/a") || tokens[2].equals("MarketCap"))
					return false;
				return true;
			}
		});

		// define mapToPair to create a Tuple<String, Tuple2<Double, String>>
		JavaPairRDD<String, Tuple2<Double, String>> pairs = filters
				.mapToPair(new PairFunction<String, String, Tuple2<Double, String>>() {
					private final static double MILLION = 1000000.00;
					public final static String NO_INFO = "n/a";

					@Override
					public Tuple2<String, Tuple2<Double, String>> call(String word) throws Exception {
						String[] tokens = word.split(",");
						String capStr = tokens[2];
						Double cap = 0.0;

						capStr = capStr.replace("$", ""); // Remove "$"

						if (capStr.contains("M")) {
							capStr = capStr.replace("M", ""); // Remove "M"
							cap = (Double.parseDouble(capStr)) * MILLION; // Convert
																			// to
																			// numeric
																			// form
						} else
							cap = Double.parseDouble(capStr);

						return new Tuple2<String, Tuple2<Double, String>>(tokens[0],
								new Tuple2<Double, String>(cap, tokens[1]));
					}
				});

		/*
		 * define a reduceByKey by providing an *associative* operation that can
		 * reduce any 2 values down to 1 (e.g. two integers sum to one integer).
		 * The operation reduces all values for each key to one value.
		 */

		JavaPairRDD<String, Tuple2<Double, String>> counts = pairs
				.reduceByKey(new Function2<Tuple2<Double, String>, Tuple2<Double, String>, Tuple2<Double, String>>() {

					@Override
					public Tuple2<Double, String> call(Tuple2<Double, String> a, Tuple2<Double, String> b)
							throws Exception {

						// Sum up the capitalization for all the tickers for
						// this sector
						Double sum = a._1 + b._1;

						return new Tuple2<Double, String>(sum, a._2.toString() + "," + b._2.toString());
					}
				});

		// Count the number of tickers in each sector
		JavaPairRDD<String, Tuple2<String, String>> combined = counts
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<Double, String>>, String, Tuple2<String, String>>() {

					private final static String recordRegex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

					@Override
					public Tuple2<String, Tuple2<String, String>> call(Tuple2<String, Tuple2<Double, String>> in)
							throws Exception {
						String tickers = in._2._2;
						String[] tokens = tickers.split(recordRegex, -1);
						long tickerCount = tokens.length;
						return new Tuple2<String, Tuple2<String, String>>(in._1, new Tuple2<String, String>(
								"Sector capital = " + in._2._1, "Ticker count = " + tickerCount + "," + in._2._2));
					}
				});

		// Sort the output by keys.
		JavaPairRDD<String, Tuple2<String, String>> sortedCounts = combined.sortByKey();

		/*-
		* start the job by indicating a save action
		* The following starts the job and tells it to save the output to
		outputPath
		*/
		sortedCounts.saveAsTextFile(outputPath);

		// done
		sc.close();
	}

}
