import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import scala.Function1;
import scala.Serializable;

import java.util.function.Function;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.sum;

public class Runner {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir","C:\\hadoop" );
        String path = "input/sample.txt";

        LogProcessing logProcessing = new LogProcessing(path);
        SparkConf conf = new SparkConf().setAppName("Request reader")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        
       JavaRDD<RequestInformation> requestsData = logProcessing.getRequestsInfo();
       JavaPairRDD<String, Integer> countData = logProcessing.getBrowserInfo();

        countData.foreach(data1 -> {
            System.out.println("Browser: "+ data1._1() + " count: " + data1._2());
        });

        try {
            requestsData.coalesce(1).saveAsTextFile("Requests info");
        }
        catch (Exception ex) {
            System.out.println(ex.getMessage());
        }

    }
}
