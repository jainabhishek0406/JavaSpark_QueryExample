package main.java;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author Abhishek Jain
 *
 * https://allaboutscala.com/big-data/spark/
 * https://spark.apache.org/docs/latest/sql-getting-started.html
 */
public class DataProcessor {
    public void processListData(SparkSession sparkSession) throws AnalysisException {
        System.out.println("##### Inside processListData #####");
        System.out.println("sparkSession = " + sparkSession);

        Dataset<Row> dataFrame1 = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("dateFormat","yyyy-MM-dd HH:mm:ss")
                .load("data/100 Sales Records.csv");

        Dataset<Row> dataFrame2 = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("dateFormat","yyyy-MM-dd HH:mm:ss")
                .load("data/1000 Sales Records.csv");

        //dataFrame1.printSchema();

        dataFrame1.createTempView("salesRecord1");
        //dataFrame1 = sparkSession.sql("select * from salesRecord1");
        //dataFrame1.show();

        dataFrame2.createTempView("salesRecord2");
        //dataFrame2 = sparkSession.sql("select * from salesRecord2");
        //dataFrame2.show();

        //Dataset<Row> dataFrame3 = null;
        //dataFrame3.createOrReplaceTempView("salesRecordJoin");
        sparkSession.sql("select * from salesRecord2").show(10);
        //sparkSession.sql("select t1.Region, t1.Country, t1.Order ID from salesRecord2 t1 join on salesRecord1 t2 where t2.Region=t1.Region").show(10);
        //dataFrame3.show();
    }
}


/*  working
    df.printSchema();

    df.select("maker", "model").show();
    df.groupBy("maker").count().show(10);
    df.show();
    df.show(5);

    df.createOrReplaceTempView("car");
    df = sparkSession.sql("select * from car");
    df.show(5);
 */