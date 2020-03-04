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
import static org.apache.spark.sql.functions.to_timestamp;
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
    outputData = outputData.withColumn("shift_report_date_converted",to_timestamp(col("shift_report_date"),"d/M/yyyy h:mm:ss a"));
    outputData.schema().printTreeString();
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
    // importFileToDB(spark,file3,"PATIENT_MEDICINE","");
    // importFileToDB(spark,file4,"PATIENT_IMMUNISATION","");
    // importShiftMeasuresToDB(spark,file5);
    importTANDMToDB(spark,file6);

    spark.stop();
  }
    
  /**
   * @param args the command line arguments
   */
  public static void main(String[] args) throws Exception {
    testFile();
  }
  
}
