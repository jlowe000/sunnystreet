/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package viz.load;

import java.io.File;
import java.util.*;
import org.apache.spark.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.unix_timestamp;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.posexplode;
import static org.apache.spark.sql.functions.split;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.FloatType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.FeatureHasher;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.BinaryLogisticRegressionTrainingSummary;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;

/**
 *
 * @author jlowe000
 */
public class SparkLoad {

  private static final String workdir = System.getenv("MAVEN_PROJECTBASEDIR");

  
  private static void saveToDB(Dataset<Row> ds, String dbtable) {
    // JDBC configuration for Postgres
    ds.write()
      .format("jdbc")
      .option("url","jdbc:postgresql:viz4socialgood")
      .option("user", "viz4socialgood")
      .option("password", "viz4socialgood")
      .option("dbtable", dbtable)
      .mode(SaveMode.Append)
      .save();
  }

  private static void importTANDMToDB(SparkSession spark, String file) {
    List<org.apache.spark.sql.types.StructField> listOfStructField = new ArrayList<org.apache.spark.sql.types.StructField>();
    listOfStructField.add(DataTypes.createStructField("primary", DataTypes.StringType, true));
    listOfStructField.add(DataTypes.createStructField("second", DataTypes.StringType, true));
    StructType structType = DataTypes.createStructType(listOfStructField);

    Dataset<Row> csvData = spark.read()
	                        .option("header","true")
	                        .option("inferSchema","true")
				.csv(file).cache();
    for (String ofn : csvData.schema().fieldNames()) {
      String nfn = ofn.toLowerCase().replaceAll("[^a-zA-Z0-9]","_");
      csvData = csvData.withColumnRenamed(ofn,nfn);
    }

    Dataset<Row> masterData = csvData.select("action","id","start_time","end_time","hour_of_day","duration_sec","duration_hhmmss","observer","observed","location")
                                     .withColumn("start_time_converted",to_timestamp(col("start_time"),"yyyy-MMM-dd HH:mm:ss"))
                                     .withColumn("end_time_converted",to_timestamp(col("end_time"),"yyyy-MMM-dd HH:mm:ss"));
    masterData.schema().printTreeString();
    masterData.write()
      .format("jdbc")
      .option("url","jdbc:postgresql:viz4socialgood")
      .option("user", "viz4socialgood")
      .option("password", "viz4socialgood")
      .option("dbtable", "TANDM_MASTER")
      .mode(SaveMode.Overwrite)
      .save();

    Dataset<Row> taskData = csvData.select(col("action"),col("id"),col("observer"),col("task"))
	                           .withColumn("primary_task", split(col("task"),"\\|").getItem(0))
                                   .withColumn("secondary_task", split(col("task"),"\\|").getItem(1))
				   .drop(col("task"));
    for (String ofn : taskData.schema().fieldNames()) {
      String nfn = ofn.toLowerCase().replaceAll("[^a-zA-Z0-9]","_");
      taskData = taskData.withColumnRenamed(ofn,nfn);
    }

    taskData.schema().printTreeString();
    taskData.write()
      .format("jdbc")
      .option("url","jdbc:postgresql:viz4socialgood")
      .option("user", "viz4socialgood")
      .option("password", "viz4socialgood")
      .option("dbtable", "TANDM_TASK")
      .mode(SaveMode.Overwrite)
      .save();
  }

  private static void importFileAggToDB(SparkSession spark, String file, String name) {
    Dataset<Row> csvData = spark.read()
	                        .option("header","true")
	                        .option("inferSchema","true")
				.csv(file).cache();
    System.out.println(csvData.count());
    csvData.schema().printTreeString();
    Dataset<Row> outputData = csvData;
    for (String ofn : outputData.schema().fieldNames()) {
      String nfn = ofn.toLowerCase().replaceAll("[^a-zA-Z0-9]","_");
      outputData = outputData.withColumnRenamed(ofn,nfn);
    }
    outputData.schema().printTreeString();
    outputData.filter(name.toLowerCase()+"_indicator_value <> 0")
	      .groupBy("patientlinkid",name.toLowerCase()+"_indicator_name",name.toLowerCase()+"_indicator_value") 
	      .max("patientid")
	      .withColumnRenamed("max(patientid)","patientid")
      .write()
      .format("jdbc")
      .option("url","jdbc:postgresql:viz4socialgood")
      .option("user", "viz4socialgood")
      .option("password", "viz4socialgood")
      .option("dbtable", "PATIENT_"+name+"_AGG")
      .mode(SaveMode.Overwrite)
      .save();
  }

  private static void importFileToDB(SparkSession spark, String file, String name, String col, String... cols) {
    Dataset<Row> csvData = spark.read()
	                        .option("header","true")
	                        .option("inferSchema","true")
				.csv(file).cache();
    System.out.println(csvData.count());
    csvData.schema().printTreeString();
    Dataset<Row> outputData = null;
    if (cols.length > 0) {
      outputData = csvData.select(col, cols);
    } else if (col != null && !"".equals(col)) {
      outputData = csvData.select(col);
    } else {
      outputData = csvData;
    }
    for (String ofn : outputData.schema().fieldNames()) {
      String nfn = ofn.toLowerCase().replaceAll("[^a-zA-Z0-9]","_");
      outputData = outputData.withColumnRenamed(ofn,nfn);
    }
    outputData.schema().printTreeString();
    outputData.write()
      .format("jdbc")
      .option("url","jdbc:postgresql:viz4socialgood")
      .option("user", "viz4socialgood")
      .option("password", "viz4socialgood")
      .option("dbtable", name)
      .mode(SaveMode.Overwrite)
      .save();
  }

  private static void importShiftMeasuresToDB(SparkSession spark, String file) {
    Dataset<Row> csvData = spark.read()
	                        .option("header","true")
	                        .option("inferSchema","true")
				.csv(file).cache();
    System.out.println(csvData.count());
    csvData.schema().printTreeString();
    Dataset<Row> outputData = csvData;
    for (String ofn : outputData.schema().fieldNames()) {
      String nfn = ofn.toLowerCase().replaceAll("[^a-zA-Z0-9]","_");
      outputData = outputData.withColumnRenamed(ofn,nfn);
    }
    outputData = outputData.withColumn("shift_report_date_converted",to_date(col("shift_report_date"),"d/M/yyyy h:mm:ss a"));
    outputData = outputData.withColumn("rd1",to_timestamp(col("shift_report_date"),"d/M/yyyy h:mm:ss a"));
    outputData = outputData.withColumn("st1",to_timestamp(col("start_time"),"30/12/1899 h:mm:ss a"));
    outputData = outputData.withColumn("et1",to_timestamp(col("end_time"),"30/12/1899 h:mm:ss a"));
    Dataset<Row> dtData = outputData.selectExpr("shift_report_id", "rd1", "to_timestamp(unix_timestamp(shift_report_date_converted)+unix_timestamp(st1)+36000) as st2", "to_timestamp(unix_timestamp(shift_report_date_converted)+unix_timestamp(et1)+36000) as et2", "to_timestamp(unix_timestamp(shift_report_date_converted)+unix_timestamp(st1)+36000-86400) as st3", "to_timestamp(unix_timestamp(shift_report_date_converted)+unix_timestamp(et1)+36000-86400) as et3"); // Allowing for 10 GMT locale offset
    Dataset<Row> dtsData = dtData.select(col("shift_report_id").alias("srid"),when(col("rd1").lt(col("st2")),col("st3")).otherwise(col("st2")).alias("start_time_converted"),when(col("rd1").lt(col("et2")),col("et3")).otherwise(col("et2")).alias("end_time_converted"));
    outputData = outputData.join(dtsData,outputData.col("shift_report_id").equalTo(dtsData.col("srid")));
    outputData = outputData.drop("rd1","st1","et1","srid");
    outputData.show(1);
    outputData.write()
      .format("jdbc")
      .option("url","jdbc:postgresql:viz4socialgood")
      .option("user", "viz4socialgood")
      .option("password", "viz4socialgood")
      .option("dbtable", "SHIFT_MEASURES")
      .mode(SaveMode.Overwrite)
      .save();
  }

  private static void importPatientFieldsToDB(SparkSession spark, Dataset<Row> outputData, String colPrefix) {
    String[] fieldnames = outputData.schema().fieldNames();
    List<String> fieldnamelist = new ArrayList<String>(Arrays.asList(fieldnames));
    for (Iterator<String> ii = fieldnamelist.iterator(); ii.hasNext(); ) {
      String fieldname = ii.next();
      if ("patientid".equals(fieldname) || "patientlinkid".equals(fieldname) || "quartername".equals(fieldname)) {
        ii.remove();
      } else if (colPrefix != null && fieldname.startsWith(colPrefix+"_")) {
        ii.remove();
      } else if (colPrefix == null && (!fieldname.startsWith("diag_") && !fieldname.startsWith("imm_") && !fieldname.startsWith("mbs_") && !fieldname.startsWith("meas_") && !fieldname.startsWith("med_"))) {
        ii.remove();
      }
    }
    // System.out.println(fieldnamelist);
    outputData = outputData.drop(fieldnamelist.toArray(new String[0]));
    outputData.schema().printTreeString();
    outputData.write()
      .format("jdbc")
      .option("url","jdbc:postgresql:viz4socialgood")
      .option("user", "viz4socialgood")
      .option("password", "viz4socialgood")
      .option("dbtable", colPrefix == null ? "PATIENT_MASTER" : "PATIENT_"+colPrefix.toUpperCase()+"_METRICS")
      .mode(SaveMode.Overwrite)
      .save();
  }

  private static void importPatientToDB(SparkSession spark, String file) {
    Dataset<Row> csvData = spark.read()
	                        .option("header","true")
	                        .option("inferSchema","true")
				.csv(file).cache();
    // System.out.println(csvData.count());
    // csvData.schema().printTreeString();
    Dataset<Row> outputData = csvData;
    for (String ofn : outputData.schema().fieldNames()) {
      String nfn = ofn.toLowerCase().replaceAll("[^a-zA-Z0-9]","_");
      outputData = outputData.withColumnRenamed(ofn,nfn);
    }
    importPatientFieldsToDB(spark,outputData,null);
    importPatientFieldsToDB(spark,outputData,"diag");
    importPatientFieldsToDB(spark,outputData,"imm");
    importPatientFieldsToDB(spark,outputData,"mbs");
    importPatientFieldsToDB(spark,outputData,"meas");
    importPatientFieldsToDB(spark,outputData,"med");
    /*
    outputData.schema().printTreeString();
    outputData.write()
      .format("jdbc")
      .option("url","jdbc:postgresql:viz4socialgood")
      .option("user", "viz4socialgood")
      .option("password", "viz4socialgood")
      .option("dbtable", name)
      .mode(SaveMode.Overwrite)
      .save();
    */
  }

  public static void testFile() throws Exception {
    String file1 = "/home/jlowe000/repos/sunnystreet/data/sunny-street-patients.csv";
    String file2 = "/home/jlowe000/repos/sunnystreet/data/sunny-street-patient-diagnosis.csv";
    String file3 = "/home/jlowe000/repos/sunnystreet/data/sunny-street-patient-medicine.csv";
    String file4 = "/home/jlowe000/repos/sunnystreet/data/sunny-street-patient-immunisation.csv";
    String file5 = "/home/jlowe000/repos/sunnystreet/data/campfire-shift-measures.csv";
    String file6 = "/home/jlowe000/repos/sunnystreet/data/sunny-street-tandm.csv";
    SparkSession spark = SparkSession.builder().appName("Simple Application").config("spark.master","local").getOrCreate();

    // importPatientToDB(spark,file1);
    // importFileToDB(spark,file2,"PATIENT_DIAGNOSIS","");
    // importFileAggToDB(spark,file2,"DIAGNOSIS");
    // importFileToDB(spark,file3,"PATIENT_MEDICINE","");
    // importFileAggToDB(spark,file3,"MEDICINE");
    // importFileToDB(spark,file4,"PATIENT_IMMUNISATION","");
    // importFileAggToDB(spark,file4,"IMMUNISATION");
    importShiftMeasuresToDB(spark,file5);
    // importTANDMToDB(spark,file6);

    spark.stop();
  }
    
  /**
   * @param args the command line arguments
   */
  public static void main(String[] args) throws Exception {
    testFile();
  }
  
}
