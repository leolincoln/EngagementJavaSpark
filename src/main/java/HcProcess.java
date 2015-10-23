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

//required for accessing cassandra from worker node. 
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.google.common.collect.Lists;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class HcProcess implements Serializable {
	private static final JavaDoubleRDD cassndraRowsRDD = null;
	private transient SparkConf conf;
	private Integer subjectNum = 4;
    //tables has all the data in. Hardcoded the prefix.
    private String prefix = "piemandata";
    private int[] indexes = {0,1,2,3,4,5,6,7,8,9};

	// private int xSize, ySize, zSize, subjectSize;

	private HcProcess(SparkConf conf) {
		this.conf = conf;
	}

	private synchronized void run() {
        //Create java spark context
		JavaSparkContext sc = new JavaSparkContext(conf);
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

    /**
     *
     * @param id -- string of the id containing x|y|z
     * @return String of cql to the where clause.
     * E.g. input id: 1|1|1
     *      output: x=1 and y=1 and z=1
     */
    public String cqlString(String id){
        String[] xyz = id.split("|");
        return "x="+xyz[0]+" and y="+xyz[1]+" and z="+xyz[2];
    }

	/**
	 * 
	 * @param sc
	 *            -- JavaSparkContext passed from main.
	 */
	public synchronized void testGetData(final JavaSparkContext sc) {
		final long startTime_mapping = System.currentTimeMillis();
		/*
		 * JavaRDD<String> rdd = javaFunctions(sc).cassandraTable("engagement",
		 * "test", mapRowTo(Test.class)).map(new Function<Test, String>() {
		 * 
		 * @Override public String call(Test test) throws Exception { return
		 * test.toString(); } }); System.out.println("testData: \n" +
		 * StringUtils.join(rdd.toArray(), "\n"));
		 */

		JavaPairRDD<String, List<Integer>> s = javaFunctions(sc)
				.cassandraTable("engagement", prefix + subjectNum,
						mapRowTo(HcList.class)).limit(100L).mapToPair(
                        new PairFunction<HcList, String, List<Integer>>() {
                            private static final long serialVersionUID = 1L;

                            public Tuple2<String, List<Integer>> call(HcList t)
                                    throws Exception {
                                return new Tuple2<String, List<Integer>>(t
                                        .getId(), t.getData());
                            }
                        });
		s.persist(StorageLevel.MEMORY_ONLY());
		final long endTime_mapping = System.currentTimeMillis();
		System.out.println("Mapping To Double list finished, time is: "
				+ (endTime_mapping - startTime_mapping));

		// taking the cartesian product of a id,listOfDoubleData pair. final
		long startTime_cartesian = System.currentTimeMillis();

		JavaPairRDD<Tuple2<String, List<Integer>>, Tuple2<String, List<Integer>>> cartProduct = s
				.cartesian(s);
		// System.out.println("cartData: ");
		// System.out.println(StringUtils.join(cartProduct.toArray(), "\n"));
		final long endTime_cartesian = System.currentTimeMillis();
		System.out.println("cartesian finished, time: "
				+ (endTime_cartesian - startTime_cartesian));

		final long startTime_corr = System.currentTimeMillis();
		
		JavaRDD<HcResults> corrData = cartProduct
				.map(new Function<Tuple2<Tuple2<String, List<Integer>>, Tuple2<String, List<Integer>>>, HcResults>() {
					public HcResults call(
							Tuple2<Tuple2<String, List<Integer>>, Tuple2<String, List<Integer>>> t)
							throws Exception {
                        String table_name = "";
                        Double sum_corr= 0.0;
                        double c = 0.0;
						//now go through all indexes from indexes.
                        for(int index:indexes){
                            table_name = prefix+index;
                            //use the above table name to get the corr data from the same x,y,z
                            String cqlString1 = cqlString(t._1()._1());
                            String cqlString2 = cqlString(t._2()._1());
                            List<Integer> x = javaFunctions(sc).cassandraTable(sc.getConf().get("keyspaceName"),table_name,mapRowTo(HcList.class)).where(cqlString1).map(new Function<HcList, List<Integer>>() {
                                public List<Integer> call(HcList hcList) throws Exception {
                                    return hcList.getData();
                                }
                            }).first();
                            List<Integer> y = javaFunctions(sc).cassandraTable(sc.getConf().get("keyspaceName"),table_name,mapRowTo(HcList.class)).where(cqlString2).map(new Function<HcList, List<Integer>>() {
                                public List<Integer> call(HcList hcList) throws Exception {
                                    return hcList.getData();
                                }
                            }).first();
                            PearsonsCorrelation pc = new PearsonsCorrelation();
                            c = pc.correlation(getDoubleArray(x),
                                    getDoubleArray(y));
                            //determine if we want to keep the value or not.
                            sum_corr +=c;
                        }
                        // if the total correlation is not greater than 0.5
                        // then ditch the result.
                        if (sum_corr/ indexes.length <0.5){
                            return new HcResults("no","no",0.0);
                        }
                        else{
                            return new HcResults(t._1()._1(), t._2()._1(), sum_corr/ indexes.length );
                        }
					}
				});
		final long endTime_corr = System.currentTimeMillis();
		System.out.println("corrData finished. Time: "
				+ (endTime_corr - startTime_corr));
		final long startTime_writingCorr = System.currentTimeMillis();
		System.out.println("starting writing output to corrdata table");

        javaFunctions(corrData).writerBuilder(sc.getConf().get("keyspaceName"),
                sc.getConf().get("tableName"), mapToRow(HcResults.class))
				.saveToCassandra();
		
		final long endTime_writingCorr = System.currentTimeMillis();
		System.out.println("writing corr finished. Time: "
				+ (endTime_writingCorr - startTime_writingCorr));

	}

    //from a list of Integer array get a list of double
	private synchronized double[] getDoubleArray(List<Integer> inArray) {
		double[] result = new double[inArray.size()];
		int i = 0;
		for (int el : inArray) {
			result[i] = el + 0.0;
			i++;
		}
		return result;
	}


	public synchronized static void main(String args[]) {

		/*
		 * to set the username: .set("spark.cassandra.username", "cassandra")
		 * //Optional to set the password: .set("spark.cassandra.password",
		 * "cassandra") //Optional
		 */
		SparkConf conf = new SparkConf();
		conf.setAppName("HcProcess_new");
		// local[4] is not a spark cluster.
		//conf.setMaster("spark://wolf.iems.northwestern.edu:7077");
		// cub0 is the cassandra cluster
		//conf.set("spark.cassandra.username", "cassandra"); // Optional
		//conf.set("spark.cassandra.password", "cassandra"); // Optional
		// conf.set("spark.cassandra.connection.host", "cub0,cub2,cub3,cub1");
		conf.set("spark.cassandra.connection.timeout_ms","20000");
		conf.set("spark.cassandra.connection.host", "cub0,cub1,cub2,cub3");
		conf.set("spark.cassandra.auth.username", "cassandra");
		conf.set("spark.cassandra.auth.password", "cassandra");
		conf.set("spark.executor.memory", "20g");
		conf.set("spark.task.maxFailures", "40");
		conf.set("keyspaceName", "engagement");
		conf.set("tableName", "pieman_corr_new");
		conf.set("spark.cores.max", "16");
		//conf.set("spark.cassandra.input.consistency.level", "ONE");
		//conf.set("spark.cassandra.input.split.size","50000");
		//conf.set("spark.cassandra.output.concurrent.writes", "1");
		conf.set("spark.cassandra.output.batch.size.bytes","1024");
		//conf.set("spark.cassandra.output.consistency.level", "ONE");
		// default is 1000
		//conf.set("spark.cassandra.input.page.row.size", "10");
		// default is 100000
		// concurrent writes for cassandra is specified in cassandra.yaml which
		// has 32 as the max value.
		//conf.set("spark.cassandra.output.concurrent.writes", "32");
		//conf.set("spark.cassandra.output.throughput_mb_per_sec", "800");
		conf.set("spark.cassandra.connection.timeout_ms","20000");
		conf.set("spark.cassandra.read.timeout_ms","20000");
		// optional
		// conf.set("spark.cassandra.output.batch.size.rows", "1");
		//conf.set("spark.scheduler.mode", "FAIR");
		HcProcess app = new HcProcess(conf);
		app.run();
	}
}
