# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %md
# MAGIC **Accessing Mount Point Notebook**

# COMMAND ----------

# MAGIC %run "/Workspace/Users/chougulesuyash24@gmail.com/Small_Bank_Project/Mount_Point_Create"

# COMMAND ----------

# MAGIC %md
# MAGIC **Accessing Utility Function Notebook**

# COMMAND ----------

# MAGIC %run "/Workspace/Users/chougulesuyash24@gmail.com/Small_Bank_Project/Utility_Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading All files from Raw Layer**

# COMMAND ----------

dbutils.fs.ls("/mnt/raw")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading all files from raw/central_data/Sangli_Dist_Branch**

# COMMAND ----------

dbutils.fs.ls("/mnt/raw/central_data/Sangli_Dist_Branch/01032025")

# COMMAND ----------

path1 = "dbfs:/mnt/raw/central_data/Sangli_Dist_Branch/01032025/sangli_01032025.csv"
path2 = "dbfs:/mnt/raw/central_data/Sangli_Dist_Branch/01032025/vishrambag_01032025.csv"
path3 = "dbfs:/mnt/raw/central_data/Sangli_Dist_Branch/01032025/vijaynagar_01032025.csv"
path4 = "dbfs:/mnt/raw/central_data/Sangli_Dist_Branch/01032025/nandre_01032025.csv"
path5 = "dbfs:/mnt/raw/central_data/Sangli_Dist_Branch/01032025/tasgaon_01032025.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating Dataframe for all branch files present in Sangli_Dist_Branch**
# MAGIC

# COMMAND ----------

sangli_df = spark.read.csv(path1, header=True, inferSchema=True)
vishrambag_df = spark.read.csv(path2, header=True, inferSchema=True)
vijaynagar_df = spark.read.csv(path3, header=True, inferSchema=True)
nandre_df = spark.read.csv(path4, header=True, inferSchema=True)
tasgaon_df = spark.read.csv(path5, header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC **Display Dataframes**

# COMMAND ----------

sangli_df.display()
vishrambag_df.display()
vijaynagar_df.display()
nandre_df.display()
tasgaon_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Convert only Timestamp format columns to date format**

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import date

# COMMAND ----------

from pyspark.sql.functions import *
sangli_df = sangli_df.withColumn("Account_Opening_Year_and_Date",to_date("Account_Opening_Year_and_Date")) \
    .withColumn("Deposit_Date",to_date("Deposit_Date"))\
    .withColumn("Loan_Start_Date",to_date("Loan_Start_Date"))\
    .withColumn("Loan_End_Date",to_date("Loan_End_Date"))\
    .withColumn("ID_Issue_Date",to_date("ID_Issue_Date"))
sangli_df.display()

# COMMAND ----------

vishrambag_df = vishrambag_df.withColumn("ID_Issue_Date",to_date("ID_Issue_Date"))\
    .withColumn("Account_Opening_Year_and_Date",to_date("Account_Opening_Year_and_Date"))\
        .withColumn("Deposit_Date",to_date("Deposit_Date"))\
            .withColumn("Loan_Start_Date",to_date("Loan_Start_Date"))\
                 .withColumn("Loan_End_Date",to_date("Loan_End_Date"))

vishrambag_df.display()

# COMMAND ----------

vijaynagar_df = vijaynagar_df.withColumn("ID_Issue_Date",to_date("ID_Issue_Date"))\
    .withColumn("Account_Opening_Year_and_Date",to_date("Account_Opening_Year_and_Date"))\
    .withColumn("Deposit_Date",to_date("Deposit_Date"))\
    .withColumn("Loan_Start_Date",to_date("Loan_Start_Date"))\
    .withColumn("Loan_End_Date",to_date("Loan_End_Date"))
vijaynagar_df.display()

# COMMAND ----------

nandre_df = nandre_df.withColumn("ID_Issue_Date",to_date("ID_Issue_Date"))\
    .withColumn("Account_Opening_Year_and_Date",to_date("Account_Opening_Year_and_Date"))\
    .withColumn("Deposit_Date",to_date("Deposit_Date"))\
    .withColumn("Loan_Start_Date",to_date("Loan_Start_Date"))\
    .withColumn("Loan_End_Date",to_date("Loan_End_Date"))
    
nandre_df.display()

# COMMAND ----------

tasgaon_df=tasgaon_df.withColumn("ID_Issue_Date",to_date("ID_Issue_Date"))\
    .withColumn("Account_Opening_Year_and_Date",to_date("Account_Opening_Year_and_Date"))\
    .withColumn("Deposit_Date",to_date("Deposit_Date"))\
    .withColumn("Loan_Start_Date",to_date("Loan_Start_Date"))\
    .withColumn("Loan_End_Date",to_date("Loan_End_Date"))

tasgaon_df.display()

# COMMAND ----------

sangli_df.display()
vishrambag_df.display()
vijaynagar_df.display()
nandre_df.display()
tasgaon_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **In sangli_df serial_no row wise sort**

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.window import Window

window = Window.orderBy(col("Serial_No"))
sangli_df = sangli_df.withColumn("Serial_No", row_number().over(window))
sangli_df.display()

# COMMAND ----------

sangli_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Rename columns**

# COMMAND ----------

sangli_df = sangli_df.withColumnRenamed("Deposit_Date", "FD_Deposit_Date")\
                     .withColumnRenamed("Tenure", "FD_Tenure")\
                     .withColumnRenamed("Interest_Rate", "FD_Interest_Rate")\
                     .withColumnRenamed("Interest_Payout_Frequency", "FD_Interest_Payout_Frequency")\
                     .withColumnRenamed("Linked_Account_Type", "FD_Linked_Account_Type")\
                     .withColumnRenamed("Interest_Rate_Type", "Loan_Interest_Rate_Type")\
                    .withColumnRenamed("Interest_Rate_Offered", "Loan_Interest_Rate_Offered")\
                    .withColumnRenamed("Credit_Score", "Loan_Credit_Score")\
                    .withColumnRenamed("Repayment_Method", "Loan_Repayment_Method")\
                    .withColumnRenamed("Collateral_Type", "Loan_Collateral_Type")
                    
sangli_df.display()

# COMMAND ----------

vishrambag_df = vishrambag_df.withColumnRenamed("Deposit_Date", "FD_Deposit_Date")\
                     .withColumnRenamed("Tenure", "FD_Tenure")\
                     .withColumnRenamed("Interest_Rate", "FD_Interest_Rate")\
                     .withColumnRenamed("Interest_Payout_Frequency", "FD_Interest_Payout_Frequency")\
                     .withColumnRenamed("Linked_Account_Type", "FD_Linked_Account_Type")\
                     .withColumnRenamed("Interest_Rate_Type", "Loan_Interest_Rate_Type")\
                    .withColumnRenamed("Interest_Rate_Offered", "Loan_Interest_Rate_Offered")\
                    .withColumnRenamed("Credit_Score", "Loan_Credit_Score")\
                    .withColumnRenamed("Repayment_Method", "Loan_Repayment_Method")\
                    .withColumnRenamed("Collateral_Type", "Loan_Collateral_Type")
                    
vishrambag_df.display()

# COMMAND ----------

vijaynagar_df = vijaynagar_df.withColumnRenamed("Deposit_Date", "FD_Deposit_Date")\
                     .withColumnRenamed("Tenure", "FD_Tenure")\
                     .withColumnRenamed("Interest_Rate", "FD_Interest_Rate")\
                     .withColumnRenamed("Interest_Payout_Frequency", "FD_Interest_Payout_Frequency")\
                     .withColumnRenamed("Linked_Account_Type", "FD_Linked_Account_Type")\
                     .withColumnRenamed("Interest_Rate_Type", "Loan_Interest_Rate_Type")\
                    .withColumnRenamed("Interest_Rate_Offered", "Loan_Interest_Rate_Offered")\
                    .withColumnRenamed("Credit_Score", "Loan_Credit_Score")\
                    .withColumnRenamed("Repayment_Method", "Loan_Repayment_Method")\
                    .withColumnRenamed("Collateral_Type", "Loan_Collateral_Type")
                    
vijaynagar_df.display()

# COMMAND ----------

nandre_df = nandre_df.withColumnRenamed("Deposit_Date", "FD_Deposit_Date")\
                     .withColumnRenamed("Tenure", "FD_Tenure")\
                     .withColumnRenamed("Interest_Rate", "FD_Interest_Rate")\
                     .withColumnRenamed("Interest_Payout_Frequency", "FD_Interest_Payout_Frequency")\
                     .withColumnRenamed("Linked_Account_Type", "FD_Linked_Account_Type")\
                     .withColumnRenamed("Interest_Rate_Type", "Loan_Interest_Rate_Type")\
                    .withColumnRenamed("Interest_Rate_Offered", "Loan_Interest_Rate_Offered")\
                    .withColumnRenamed("Credit_Score", "Loan_Credit_Score")\
                    .withColumnRenamed("Repayment_Method", "Loan_Repayment_Method")\
                    .withColumnRenamed("Collateral_Type", "Loan_Collateral_Type")
                    
nandre_df.display()

# COMMAND ----------

tasgaon_df = tasgaon_df.withColumnRenamed("Deposit_Date", "FD_Deposit_Date")\
                     .withColumnRenamed("Tenure", "FD_Tenure")\
                     .withColumnRenamed("Interest_Rate", "FD_Interest_Rate")\
                     .withColumnRenamed("Interest_Payout_Frequency", "FD_Interest_Payout_Frequency")\
                     .withColumnRenamed("Linked_Account_Type", "FD_Linked_Account_Type")\
                     .withColumnRenamed("Interest_Rate_Type", "Loan_Interest_Rate_Type")\
                    .withColumnRenamed("Interest_Rate_Offered", "Loan_Interest_Rate_Offered")\
                    .withColumnRenamed("Credit_Score", "Loan_Credit_Score")\
                    .withColumnRenamed("Repayment_Method", "Loan_Repayment_Method")\
                    .withColumnRenamed("Collateral_Type", "Loan_Collateral_Type")
                    
tasgaon_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **All Branches Join**

# COMMAND ----------

final_df = sangli_df.union(vishrambag_df).union(vijaynagar_df).union(nandre_df).union(tasgaon_df)
final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Write a parquet file into silver folder and in that branchwise store each parquet file**

# COMMAND ----------

from datetime import date
from datetime import datetime

ingest_date = datetime.now().strftime("%Y-%m-%d")
branches = {
    "St Stand": sangli_df,
    "Vishram Bag": vishrambag_df,
    "VijayNagar": vijaynagar_df,
    "Nandre": nandre_df,
    "Tasgaon": tasgaon_df
}
for branch, final_df in branches.items():
    folder_path = f"mnt/silver/{branch}/ingest_date={ingest_date}/"
    final_df.write.mode("overwrite").parquet(folder_path)