package old_codes;
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
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import scala.Tuple2;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.google.common.collect.Lists;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class HitchCockProcess_new implements Serializable {
	private static final JavaDoubleRDD cassndraRowsRDD = null;
	private transient SparkConf conf;

	// private int xSize, ySize, zSize, subjectSize;

	private HitchCockProcess_new(SparkConf conf) {
		this.conf = conf;

	}

	private void run() {
		System.out.println("running the new hitchcockprocess");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// this.xSize = getXSize(sc);
		// this.ySize = getYSize(sc);
		// this.zSize = getZSize(sc);
		// this.subjectSize = getSubjectSize(sc);
		testGetData(sc);

		/*
		 * SparkContextJavaFunctions functions = CassandraJavaUtil
		 * .javaFunctions(sc); JavaRDD<CassandraRow> rdd =
		 * functions.cassandraTable("engagement", "hitchcockdatatotal"); //
		 * 1/23/2015 JavaPairRDD<Integer, Integer> sizes = rdd .groupBy(new
		 * Function<CassandraRow, Integer>() {
		 * 
		 * @Override public Integer call(CassandraRow row) throws Exception {
		 * return row.getInt("subject"); } }) .mapToPair( new
		 * PairFunction<Tuple2<Integer, Iterable<CassandraRow>>, Integer,
		 * Integer>() { public Tuple2<Integer, Integer> call( Tuple2<Integer,
		 * Iterable<CassandraRow>> t) throws Exception { return new
		 * Tuple2<Integer, Integer>(t._1(), Lists.newArrayList(t._2()).size());
		 * } }); sizes.cache();
		 * 
		 * /* JavaRDD<String> rdd1; JavaRDD<String> rdd2; rdd1 =
		 * javaFunctions(sc).cassandraTable("engagement", "hitchcocktotal")
		 * .where("x=? AND y=? AND z=? ", 1, 2, 3) .map(new
		 * Function<CassandraRow, String>() {
		 * 
		 * @Override public String call(CassandraRow cassandraRow) throws
		 * Exception { return cassandraRow.toString(); } }); rdd2 =
		 * javaFunctions(sc).cassandraTable("engagement", "hitchcocktotal")
		 * .where("x=? AND y=? AND z=? ", 2, 3, 4) .map(new
		 * Function<CassandraRow, String>() {
		 * 
		 * @Override public String call(CassandraRow cassandraRow) throws
		 * Exception { return cassandraRow.toString(); } });
		 */

		/*
		 * JavaRDD<Hitchcockdatatotal> rdd1 = javaFunctions(sc).cassandraTable(
		 * "engagement", "hitchcockdatatotal",
		 * mapRowTo(Hitchcockdatatotal.class)).where( "x=? AND y=? AND z=? ", 1,
		 * 2, 3); JavaRDD<Hitchcockdatatotal> rdd2 =
		 * javaFunctions(sc).cassandraTable( "engagement", "hitchcockdatatotal",
		 * mapRowTo(Hitchcockdatatotal.class)).where( "x=? AND y=? AND z=? ", 4,
		 * 5, 6); gatherData(sc, rdd1, rdd2); compute(sc, rdd1, rdd2);
		 * showResults(sc); sc.stop();
		 */
	}

	public int getXSize(JavaSparkContext sc) {
		JavaRDD<CassandraRow> xS = javaFunctions(sc)
				.cassandraTable("engagement", "hitchcockdatatotal2")
				.select("x").distinct();
		System.out.println("X size: " + xS.count());
		return (int) xS.count();
	}

	public int getYSize(JavaSparkContext sc) {
		JavaRDD<CassandraRow> yS = javaFunctions(sc)
				.cassandraTable("engagement", "hitchcockdatatotal2")
				.where("x=?", "0").select("y").distinct();
		System.out.println("Y size: " + yS.count());
		return (int) yS.count();
	}

	public int getZSize(JavaSparkContext sc) {
		JavaRDD<CassandraRow> zS = javaFunctions(sc)
				.cassandraTable("engagement", "hitchcockdatatotal2")
				.where("x=? and y=?", "0", "0").select("z").distinct();
		System.out.println("Z size: " + zS.count());
		return (int) zS.count();
	}

	public int getSubjectSize(JavaSparkContext sc) {
		JavaRDD<CassandraRow> subjectS = javaFunctions(sc)
				.cassandraTable("engagement", "hitchcockdatatotal2")
				.select("subject").distinct();
		System.out.println("subject size: " + subjectS.count());
		return (int) subjectS.count();
	}

	/**
	 * 
	 * @param sc
	 *            -- JavaSparkContext passed from main.
	 */
	public void testGetData(JavaSparkContext sc) {
		// JavaPairRDD<Hitchcockdatatotal,Integer> newRDD =
		// javaFunctions(sc).cassandraTable("engagement","hitchcockdatatotal");
		// s stores all string->list(data) pairs. string is the id of the row.
		JavaRDD<Vector> s = javaFunctions(sc)
				.cassandraTable("engagement", "hitchcockdatatotal",
						mapRowTo(Hitchcockdatatotal_new.class))
				.where("subject =?", "0")
				.mapToPair(
						new PairFunction<Hitchcockdatatotal_new, Integer, List<Tuple2<Integer, Double>>>() {
							/**
							 * 
							 */
							private static final long serialVersionUID = 1L;

							@Override
							public Tuple2<Integer, List<Tuple2<Integer, Double>>> call(
									Hitchcockdatatotal_new t) throws Exception {
								// TODO Auto-generated method stub
								ArrayList<Tuple2<Integer, Double>> temp = new ArrayList<Tuple2<Integer, Double>>();
								temp.add(new Tuple2<Integer, Double>(t.getID(),
										t.getData() + 0.0));
								return new Tuple2<Integer, List<Tuple2<Integer, Double>>>(
										t.getTime(), temp);
							}
						})
				.reduceByKey(
						new Function2<List<Tuple2<Integer, Double>>, List<Tuple2<Integer, Double>>, List<Tuple2<Integer, Double>>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public List<Tuple2<Integer, Double>> call(
									List<Tuple2<Integer, Double>> v1,
									List<Tuple2<Integer, Double>> v2)
									throws Exception {
								ArrayList<Tuple2<Integer, Double>> temp = new ArrayList<Tuple2<Integer, Double>>();
								temp.addAll(v1);
								temp.addAll(v2);
								return temp;
							}
						})
				.mapToPair(
						new PairFunction<Tuple2<Integer, List<Tuple2<Integer, Double>>>, Integer, List<Double>>() {

							@Override
							public Tuple2<Integer, List<Double>> call(
									Tuple2<Integer, List<Tuple2<Integer, Double>>> t)
									throws Exception {
								Collections.sort(t._2,
										new TupleComparator<Double>());
								ArrayList<Double> tempArray = new ArrayList<Double>();
								for (Tuple2<Integer, Double> tupleEl : t._2) {
									tempArray.add(tupleEl._2);
								}
								// TODO Auto-generated method stub
								return new Tuple2<Integer, List<Double>>(t._1,
										tempArray);
							}

						}).sortByKey()
				.map(new Function<Tuple2<Integer, List<Double>>, Vector>() {

					@Override
					public Vector call(Tuple2<Integer, List<Double>> v1)
							throws Exception {
						return Vectors.dense(getDoubleArray(v1._2));
					}
				});
		System.out.println("Mapping To Double list finished");
		// System.out.println("printing results:");
		// System.out.println("Data as CassandraRows after converting to double lists : \n"
		// +StringUtils.join(s.toArray(), "\n"));

		// .coalesce(2000).cache();
		// taking the cartesian product of a id,listOfDoubleData pair.

		Matrix correlMatrix = Statistics.corr(s.rdd(), "pearson");
		try {
			FileWriter fw = new FileWriter("corrResults.txt", false);
			BufferedWriter bf = new BufferedWriter(fw);
			bf.write(correlMatrix.toArray().toString());
			bf.close();
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			System.out.println("hitchcock process finished");
			// s.unpersist();
		}

		/*
		 * JavaPairRDD<String, ArrayList<Integer>> arrayS0 = s0 .reduceByKey(new
		 * Function2<ArrayList<Integer>, Integer, Integer>() {
		 * 
		 * @Override public Integer call(ArrayList<Integer> v1, Integer v2)
		 * throws Exception { // TODO Auto-generated method stub return null; }
		 * 
		 * }); /* JavaRDD<String> cassandraRowsRDD = javaFunctions(sc)
		 * .cassandraTable("engagement", "hitchcockdatatotal")
		 * .where("x=? and y=? and z=? and subject=? ", "0", "0", "0",
		 * "17").map(new Function<CassandraRow, String>() {
		 * 
		 * @Override public String call(CassandraRow cassandraRow) throws
		 * Exception { return cassandraRow.toString(); } });
		 * 
		 * /* System.out.println("Data as CassandraRows at 0,0,0,17: \n" +
		 * StringUtils.join(cassndraRowsRDD.toArray(), "\n"));
		 */
		/*
		 * System.out
		 * .println("Data as CassandraRows at subject=0 and time=0: \n" +
		 * StringUtils.join(s.toArray(), "\n")); System.out
		 * .println("Data as CassandraRows after converting to vectors : \n" +
		 * StringUtils.join(corrData.toArray(), "\n"));
		 */
	}

	private double[] getDoubleArray(List<Double> inArray) {
		double[] result = new double[inArray.size()];
		int i = 0;
		for (Double el : inArray) {
			result[i] = el;
			i++;
		}
		return result;
	}

	private class TupleComparator<E> implements Comparator<Tuple2<Integer, E>>,
			Serializable {
		@Override
		public int compare(Tuple2<Integer, E> tuple1, Tuple2<Integer, E> tuple2) {
			return tuple1._1 - tuple2._1;
		}
	}

	/*
	 * public void gatherData(JavaSparkContext sc, JavaRDD<Hitchcockdatatotal>
	 * rdd1, JavaRDD<Hitchcockdatatotal> rdd2) { JavaRDD<String>
	 * cassandraRowsRDD1 = javaFunctions(sc) .cassandraTable("engagement",
	 * "hitchcocktotal") .where("x=? AND y=? AND z=? ", 1, 2, 3) .map(new
	 * Function<CassandraRow, String>() {
	 * 
	 * @Override public String call(CassandraRow cassandraRow) throws Exception
	 * { return cassandraRow.toString(); } }); JavaRDD<String> cassandraRowsRDD2
	 * = javaFunctions(sc) .cassandraTable("engagement", "hitchcocktotal")
	 * .where("x=? AND y=? AND z=? ", 1, 2, 3) .map(new Function<CassandraRow,
	 * String>() {
	 * 
	 * @Override public String call(CassandraRow cassandraRow) throws Exception
	 * { return cassandraRow.toString(); } });
	 * System.out.println("Data as CassandraRows: \n" +
	 * StringUtils.join(cassandraRowsRDD1.toArray(), "\n"));
	 */
	// another way to read in data:

	/*
	 * rdd1 = javaFunctions(sc).cassandraTable("engagement", "hitchcocktotal")
	 * .where("x=? AND y=? AND z=? ", 1, 2, 3) .map(new Function<CassandraRow,
	 * String>() {
	 * 
	 * @Override public String call(CassandraRow cassandraRow) throws Exception
	 * { return cassandraRow.toString(); } }); rdd2 =
	 * javaFunctions(sc).cassandraTable("engagement", "hitchcocktotal")
	 * .where("x=? AND y=? AND z=? ", 2, 3, 4) .map(new Function<CassandraRow,
	 * String>() {
	 * 
	 * @Override public String call(CassandraRow cassandraRow) throws Exception
	 * { return cassandraRow.toString(); } });
	 * 
	 * 
	 * } /* private void compute(JavaSparkContext sc,
	 * JavaRDD<Hitchcockdatatotal> rdd1, JavaRDD<Hitchcockdatatotal> rdd2) {
	 * 
	 * // compute the correlation using Pearson's method. Enter "spearman" for
	 * // Spearman's method. If a // method is not specified, Pearson's method
	 * will be used by default. // Double correlation =
	 * Statistics.corr(rdd1.rdd(), rdd2.rdd(), // "pearson");
	 * 
	 * }
	 */
	private void showResults(JavaSparkContext sc) {
	}

	public static void main(String args[]) {

		/*
		 * to set the username: .set("spark.cassandra.username", "cassandra")
		 * //Optional to set the password: .set("spark.cassandra.password",
		 * "cassandra") //Optional
		 */
		SparkConf conf = new SparkConf();
		conf.setAppName("hitchcockprocess");
		// local[4] is not a spark cluster.
		conf.setMaster("spark://wolf.iems.northwestern.edu:7077");
		// cub0 is the cassandra cluster
		conf.set("spark.cassandra.connection.host", "cub0");
		conf.set("spark.cassandra.auth.username", "cassandra");
		conf.set("spark.cassandra.auth.password", "cassandra");
		conf.set("spark.executor.memory", "20g");
		HitchCockProcess_new app = new HitchCockProcess_new(conf);
		app.run();
	}
}
