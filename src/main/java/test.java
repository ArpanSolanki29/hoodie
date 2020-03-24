import org.apache.hudi.QuickstartUtils.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConversions.*;
import org.apache.spark.sql.SaveMode.*;
import org.apache.hudi.DataSourceReadOptions.*;
import org.apache.hudi.DataSourceWriteOptions.*;
import org.apache.hudi.config.HoodieWriteConfig.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.DataSourceWriteOptions.*;
import static org.apache.hudi.QuickstartUtils.convertToStringList;
import static org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs;
import static org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME;
import static org.apache.spark.sql.SaveMode.Overwrite;


public class test {
    public static void main(String[] args) throws IOException {

        // inserts operations


        String tableName = "hudi_trips_cow";
        String basePath = "file:///tmp/hudi_trips_cow";
        DataGenerator dataGen = new DataGenerator();

        // sparkconfig
        SparkConf sparkConf = new SparkConf().setAppName("Print Elements of RDD")
                .setMaster("local[3]").set("spark.driver.allowMultipleContexts", "true").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        // sparkSession
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        // sparkcontext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        List<String> inserts = convertToStringList(dataGen.generateInserts(10));
        System.out.println("===============inserts==============================");
        System.out.println(inserts);
        JavaRDD<String> rdd = sc.parallelize(inserts, 3);
        Dataset<Row> df = spark.read().json(rdd);
        df.write().format("org.apache.hudi").
                options(getQuickstartWriteConfigs()).
                option(PRECOMBINE_FIELD_OPT_KEY(), "ts").
                option(RECORDKEY_FIELD_OPT_KEY(), "uuid").
                option(PARTITIONPATH_FIELD_OPT_KEY(), "partitionpath").
                option(TABLE_NAME, tableName).
                mode(Overwrite).
                save(basePath);
    }
}
