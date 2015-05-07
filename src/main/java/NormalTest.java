import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.storage.StorageLevel;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import scala.Tuple2;
import scala.collection.Iterator;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.google.common.collect.Lists;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class NormalTest implements Serializable {
	private static final JavaDoubleRDD cassndraRowsRDD = null;
	private transient SparkConf conf;
	private Integer subjectNum = 0;

	// private int xSize, ySize, zSize, subjectSize;

	private NormalTest(SparkConf conf) {
		this.conf = conf;
	}

	private synchronized void run() {
		JavaSparkContext sc = new JavaSparkContext(conf);
		testNormal(sc);
	}

	/**
	 * 
	 * @param sc
	 *            -- JavaSparkContext passed from main.
	 */
	public synchronized void testNormal(JavaSparkContext sc) {

		final long startTime_mapping = System.currentTimeMillis();
		JavaRDD<NormalResults> s = javaFunctions(sc).cassandraTable(
				"engagement", "piemandatanormalTest",
				mapRowTo(NormalResults.class));

		final long endTime_mapping = System.currentTimeMillis();
		System.out.println("Mapping To Double list finished, time is: "
				+ (endTime_mapping - startTime_mapping));

		// taking the cartesian product of a id,listOfDoubleData pair. final
		long startTime_cartesian = System.currentTimeMillis();

		final long endTime_cartesian = System.currentTimeMillis();
		System.out.println("cartesian finished, time: "
				+ (endTime_cartesian - startTime_cartesian));

		final long startTime_corr = System.currentTimeMillis();

		final long endTime_corr = System.currentTimeMillis();
		System.out.println("corrData finished. Time: "
				+ (endTime_corr - startTime_corr));
		final long startTime_writingCorr = System.currentTimeMillis();
		System.out.println("starting writing output to corrdata table");
	}

	public Boolean checkNormality(int[] a) {
		// convert int to double
		double[] aD = new double[a.length];
		for (int i = 0; i < a.length; i++) {
			aD[i] = a[i] + 0.0;
		}
		// now use the aD to test normality.
		// the mean is roughly the same with median.
		// also, about 68% of the data is in 1 std of mean so i guess 65%-71%
		// 95% is with 2 std so i guess 91%-99%
		// called the back of the envelope test
		if (percentIn(aD, 1) >= .65 && percentIn(aD, 1) <= .71
				&& percentIn(aD, 2) >= 0.9 && percentIn(aD, 2) <= 99) {
			return true;
		}
		return false;

	}

	public double getMean(double[] a) {
		double sum = 0.0;
		for (int i = 0; i < a.length; i++) {
			sum += a[i];
		}
		return sum / a.length;
	}

	public double getStd(double[] a) {
		double mean = getMean(a);
		double ssum = 0.0;
		for (int i = 0; i < a.length; i++) {
			ssum += (a[i] - mean) * (a[i] - mean);
		}
		return Math.sqrt(ssum / a.length);

	}

	/**
	 * 
	 * @param a
	 *            the input double array
	 * @param b
	 *            the range of std, either 1 or 2 in this case.
	 * @return a double percentage. without the % sign.
	 */
	public double percentIn(double[] a, double b) {
		double mean = getMean(a);
		double std = getStd(a);
		double count = 0;
		for (int i = 0; i < a.length; i++) {
			if (a[i] >= mean - std * b && a[i] <= mean + std * b) {
				count++;
			}
		}
		return count / a.length;
	}

	/**
	 * 
	 * @param inArray -- the input array as Integer
	 * @return
	 */
	private synchronized double[] getDoubleArray(List<Integer> inArray) {
		double[] result = new double[inArray.size()];
		int i = 0;
		for (int el : inArray) {
			result[i] = el + 0.0;
			i++;
		}
		return result;
	}

	private class TupleComparator<E> implements Comparator<Tuple2<Integer, E>>,
			Serializable {
		@Override
		public int compare(Tuple2<Integer, E> tuple1, Tuple2<Integer, E> tuple2) {
			return tuple1._1 < tuple2._1 ? 0 : 1;
		}
	}

	private void showResults(JavaSparkContext sc) {
	}

	public synchronized static void main(String args[]) {

		/*
		 * to set the username: .set("spark.cassandra.username", "cassandra")
		 * //Optional to set the password: .set("spark.cassandra.password",
		 * "cassandra") //Optional
		 */
		SparkConf conf = new SparkConf();
		conf.setAppName("HcProcess");
		// local[4] is not a spark cluster.
		conf.setMaster("spark://wolf.iems.northwestern.edu:7077");
		// cub0 is the cassandra cluster
		// conf.set("spark.cassandra.username", "cassandra"); // Optional
		// conf.set("spark.cassandra.password", "cassandra"); // Optional
		// conf.set("spark.cassandra.connection.host", "cub0,cub2,cub3,cub1");
		conf.set("spark.cassandra.connection.timeout_ms", "20000");
		conf.set("spark.cassandra.connection.host", "cub0,cub1,cub2,cub3");
		conf.set("spark.cassandra.auth.username", "cassandra");
		conf.set("spark.cassandra.auth.password", "cassandra");
		conf.set("spark.executor.memory", "10g");
		conf.set("spark.task.maxFailures", "20");
		conf.set("keyspaceName", "engagement");
		conf.set("tableName", "piemandatacorrresults");
		conf.set("spark.cores.max", "16");
		// conf.set("spark.cassandra.input.consistency.level", "ONE");
		// conf.set("spark.cassandra.input.split.size","50000");
		// conf.set("spark.cassandra.output.concurrent.writes", "1");
		conf.set("spark.cassandra.output.batch.size.bytes", "1024");
		// conf.set("spark.cassandra.output.consistency.level", "ONE");
		// default is 1000
		// conf.set("spark.cassandra.input.page.row.size", "10");
		// default is 100000
		// concurrent writes for cassandra is specified in cassandra.yaml which
		// has 32 as the max value.
		// conf.set("spark.cassandra.output.concurrent.writes", "32");
		// conf.set("spark.cassandra.output.throughput_mb_per_sec", "800");
		conf.set("spark.cassandra.connection.timeout_ms", "20000");
		conf.set("spark.cassandra.read.timeout_ms", "20000");
		// optional
		// conf.set("spark.cassandra.output.batch.size.rows", "1");
		// conf.set("spark.scheduler.mode", "FAIR");
		NormalTest app = new NormalTest(conf);
		app.run();
	}
}