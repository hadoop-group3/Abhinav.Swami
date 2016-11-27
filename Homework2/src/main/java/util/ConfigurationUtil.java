package util;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.spark.SparkConf;

import scala.Tuple2;

public class ConfigurationUtil {

	public static void dumpConfigurations(SparkConf conf, PrintStream ps) {

		Map<String, String> sortedConfigMap = new TreeMap<String, String>();

		for (Tuple2<String, String> entry : conf.getAll()) {
			sortedConfigMap.put(entry._1(), entry._2());
		}

		ps.println("***************** configurations ***************");
		for (Map.Entry<String, String> entry : sortedConfigMap.entrySet()) {
			ps.format("%s=%s\n", entry.getKey(), entry.getValue());
		}

		ps.println("***************** configurations ***************");
	}

	public static void setupClassLogging(Logger logger, Class<?> jobclass) throws IOException {

		// setting up a FileAppender dynamically...
		SimpleLayout layout = new SimpleLayout();
		FileAppender appender = new FileAppender(layout, jobclass.getSimpleName() + ".log", true);
		logger.addAppender(appender);

		logger.setLevel(Level.DEBUG);
	}
}
