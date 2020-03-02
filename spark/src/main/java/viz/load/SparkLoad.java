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
    // outputData = outputData.select(Arrays.asList(outputData.schema().fieldNames()).stream().map(x -> x.toLowerCase().replaceAll("[^a-zA-Z0-9]","_")).toArray(size -> new String[size]));
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
  public static void testFile() throws Exception {
    String file1 = "/home/jlowe000/repos/sunnystreet/data/sunny-street-patients.csv"; // Should be some file on your system
    String file2 = "/home/jlowe000/repos/sunnystreet/data/sunny-street-patient-diagnosis.csv"; // Should be some file on your system
    String file3 = "/home/jlowe000/repos/sunnystreet/data/sunny-street-patient-medicine.csv"; // Should be some file on your system
    String file4 = "/home/jlowe000/repos/sunnystreet/data/sunny-street-patient-immunisation.csv"; // Should be some file on your system
    String file5 = "/home/jlowe000/repos/sunnystreet/data/campfire-shift-measures.csv"; // Should be some file on your system
    SparkSession spark = SparkSession.builder().appName("Simple Application").config("spark.master","local").getOrCreate();

    importFileToDB(spark,file1,"PATIENTS","");
    importFileToDB(spark,file2,"PATIENT_DIAGNOSIS","");
    importFileToDB(spark,file3,"PATIENT_MEDICINE","");
    importFileToDB(spark,file4,"PATIENT_IMMUNISATION","");
    importFileToDB(spark,file5,"SHIFT_MEASURES","");

    spark.stop();
  }
    
  /**
   * @param args the command line arguments
   */
  public static void main(String[] args) throws Exception {
    testFile();
  }
  
}
