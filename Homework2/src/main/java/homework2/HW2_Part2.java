package homework2;

import java.util.Calendar;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;

public class HW2_Part2 {

	public static void main(String[] args) {
		/*
		 * Validate that two arguments were passed from the command line.
		 */
		if (args.length != 3) {
			System.out.println("Arguments provided:  ");
			for (String arg : args) {
				System.out.println(arg);
			}
			System.out.printf("Usage: Provide <input dir1> <input dir2> <output dir> \n");
			System.out.printf(
					"Example: data/companies/SP500-constituents-financials.csv data/companies/companylistNASDAQ.csv output/hw2_1 \n");
			System.exit(-1);
		}

		/*
		 * setup job configuration and context
		 */
		SparkConf conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.setMaster("local");
		conf.setAppName("HW2 Part 1");
		JavaSparkContext sc = new JavaSparkContext(conf);

		/* setup output */
		String outputPath = args[2] + "_" + Calendar.getInstance().getTimeInMillis();

		/* Read the lines from SP500-constituents-financials.csv */
		JavaRDD<String> rdd1 = sc.textFile(args[0]);

		/* Read the lines from companylistNASDAQ.csv */
		JavaRDD<String> rdd2 = sc.textFile(args[1]);

		// Parse out key, value pairs from the csv file
		JavaPairRDD<String, String> info1 = rdd1.mapToPair(new PairFunction<String, String, String>() {

			private final static String recordRegex = ",";
			private final static int symbolIndex = 0;

			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String[] tokens = s.split(recordRegex, -1);
				return new Tuple2<String, String>(tokens[symbolIndex], s.replace(tokens[symbolIndex] + ",", ""));
			}

		});

		// Parse out key, value pairs from the second csv file
		JavaPairRDD<String, String> info2 = rdd2.mapToPair(new PairFunction<String, String, String>() {

			private final static String recordRegex = ",";
			private final static int symbolIndex = 0;

			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String str = s.replace("\"", ""); // remove all quotes from
													// string
				String[] tokens = str.split(recordRegex, -1);
				return new Tuple2<String, String>(tokens[symbolIndex], str.replace(tokens[symbolIndex] + ",", ""));
			}

		});

		// Filter out the header
		JavaPairRDD<String, String> filteredInfo1 = info1.filter(new Function<Tuple2<String, String>, Boolean>() {

			@Override
			public Boolean call(Tuple2<String, String> s) throws Exception {

				// Filter out header
				if (s._1().equals("Symbol")) {
					return false;
				}
				return true;
			}
		});

		// Combine results based on keys. Select key present in both rdd1 and
		// rdd2
		JavaPairRDD<String, Tuple2<String, String>> joined = filteredInfo1.join(info2);

		// Update dividend, 52 week high and 52 week low values
		JavaRDD<Tuple3<String, Double, Double>> stockInfo = joined
				.map(new Function<Tuple2<String, Tuple2<String, String>>, Tuple3<String, Double, Double>>() {

					private final static String recordRegex = ",";
					private final static int symbolIndex = 0;
					private final static int divYield = 4;
					private final static int priceByEarning = 5;

					@Override
					public Tuple3<String, Double, Double> call(Tuple2<String, Tuple2<String, String>> tup)
							throws Exception {
						String[] tokens = tup._2._1.split(recordRegex, -1);
						String dividend = tokens[3];
						String low = tokens[7];
						String high = tokens[8];
						if (dividend.equals("n/a") || dividend.equals(""))
							dividend = "0.0";
						if (low.equals("n/a"))
							low = "0.0";
						if (high.equals("n/a"))
							high = "0.0";
						Double increase = ((Double.parseDouble(high) - Double.parseDouble(low))
								/ Double.parseDouble(low)) * 100;
						return new Tuple3<String, Double, Double>(tup._1, Double.parseDouble(dividend), increase);
					}

				});

		// Sort by growth
		JavaRDD<Tuple3<String, Double, Double>> sortedByGrowth = stockInfo
				.sortBy(new Function<Tuple3<String, Double, Double>, Double>() {

					@Override
					public Double call(Tuple3<String, Double, Double> info) throws Exception {
						return info._3();
					}
				}, false, 1);// false = Descending order

		// Sort by highest dividend value
		JavaRDD<Tuple3<String, Double, Double>> sortedByDiv = stockInfo
				.sortBy(new Function<Tuple3<String, Double, Double>, Double>() {

					@Override
					public Double call(Tuple3<String, Double, Double> info) throws Exception {
						return info._2();
					}
				}, false, 1);// false = Descending order

		// Print the results
		System.out.println("Total symbols available in both the files = " + stockInfo.count());

		List<Tuple3<String, Double, Double>> top10Dividends = sortedByDiv.take(10);

		System.out.println("Top 10 stock by dividend");
		for (Tuple3<String, Double, Double> element : top10Dividends)
			System.out.println(element._1() + "," + element._2());

		List<Tuple3<String, Double, Double>> top10Growth = sortedByGrowth.take(10);

		System.out.println("Top 10 stocks by growth");
		for (Tuple3<String, Double, Double> element : top10Growth)
			System.out.println(element._1() + "," + element._3() + "%");

		// Save the results
		sortedByGrowth.saveAsTextFile(outputPath);
		sc.close();
	}

}
