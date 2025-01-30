# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %md
# MAGIC **Accessing Utility Function Notebook**

# COMMAND ----------

# MAGIC %run "/Workspace/Users/chougulesuyash24@gmail.com/Small_Bank_Project/Utility_Functions"
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading All Files from silver layer**

# COMMAND ----------

dbutils.fs.ls("/mnt/silver")

# COMMAND ----------

# MAGIC %md
# MAGIC **Create paths for all branches that store each branch data**

# COMMAND ----------

path1 = r"dbfs:/mnt/silver/St Stand/ingest_date=2025-01-04/part-00000-tid-3267630513273274085-39de0b57-4184-44c6-9f01-2f42bdc4cb38-857-1.c000.snappy.parquet"
path2 = r"dbfs:/mnt/silver/Vishram Bag/ingest_date=2025-01-04/part-00000-tid-1831848516577329306-26d67c43-5141-48e3-aeb9-9452b32475a2-858-1.c000.snappy.parquet"
path3 = r"dbfs:/mnt/silver/VijayNagar/ingest_date=2025-01-04/part-00000-tid-341884640237401956-ae5c98cb-5d75-4c2e-9a7a-75c771c272d0-859-1.c000.snappy.parquet"
path4 = r"dbfs:/mnt/silver/Nandre/ingest_date=2025-01-04/part-00000-tid-4510606599949143315-f455f8e1-3982-47cd-ba7c-fa4388e655a6-860-1.c000.snappy.parquet"
path5 = r"dbfs:/mnt/silver/Tasgaon/ingest_date=2025-01-04/part-00000-tid-141973231539193801-2fe36344-bae3-481d-9fcb-2aee7ed6161d-861-1.c000.snappy.parquet"

# COMMAND ----------

# MAGIC %md
# MAGIC **Create dataframes for each branch**

# COMMAND ----------

St_Stand_df = spark.read.parquet(path1)
Vishrambag_df = spark.read.parquet(path2)
Vijaynagar_df = spark.read.parquet(path3)
Nandre_df = spark.read.parquet(path4)
Tasgaon_df = spark.read.parquet(path5)

# COMMAND ----------

# MAGIC %md
# MAGIC **Displaying all branches dataframes**

# COMMAND ----------

St_Stand_df.display()
Vishrambag_df.display()
Vijaynagar_df.display()
Nandre_df.display()
Tasgaon_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Union all dataframes into single dataframe**

# COMMAND ----------

union_df = St_Stand_df.union(Vishrambag_df).union(Vijaynagar_df).union(Nandre_df).union(Tasgaon_df)
union_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformations on Customer details columns**

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Find those records whose application gets rejcted**

# COMMAND ----------

find_app_df = union_df.filter(union_df.Application_Status == 'Rejected')
find_app_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Find those customers whose account type is current**

# COMMAND ----------

find_acc_df = union_df.filter(union_df.Account_Type == 'Current')
find_acc_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Count Number of customers whose Annual Income is greater than 600000**

# COMMAND ----------

from pyspark.sql.functions import col, count
count_ann_df = union_df.filter(union_df.Annual_Income > 600000)
print("Total Number of Customers whose Annual Income is > 600000:" ,count_ann_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC **Yearwise count total Account openings by Customer from 2014 to 2024 in branch wise.**

# COMMAND ----------

from pyspark.sql.functions import col, count, year 
union = union_df.withColumn("Year", year(col("Account_Opening_Year_and_Date")))
union.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **2014 to 2024 Account opening by customers yearwise and branchwise**

# COMMAND ----------

union = union.filter((col("Year")>= 2014) & (col("Year") <=2024))
union = union.groupBy("Year", "Branch").agg(count("Account_Number").alias("Account_Openings_By_Customer"))
union.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformations on FD Columns**

# COMMAND ----------

# MAGIC %md
# MAGIC **Calculate FD Tenure Classification**

# COMMAND ----------

from pyspark.sql.functions import when
cal_tenture_df = union_df.withColumn("FD_Tenure_Classification", 
                                    when(col("FD_Tenure") <= 12, "Short Term")
                                    .when((col("FD_Tenure") > 12) & (col("FD_Tenure") <= 60), "Medium Term")
                                    .when(col("FD_Tenure") > 60, "Long Term"))

cal_tenture_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Calculate the Maturity date, Maturity Amount, Monthly Maturity Amount and FD Status from particular column like FD Amount, Interest Rate, Deposit Date and Tenure.**

# COMMAND ----------

from pyspark.sql.functions import * # type: ignore
from pyspark.sql import functions as F # type: ignore

cal_FD_df = (
    cal_tenture_df.withColumn("FD_Maturity_Date", F.expr("add_months(`FD_Deposit_Date`, FD_Tenure)"))

    .withColumn("FD_Maturity_Amount",
                round(F.col("FD_Amount") * F.pow(1 + F.col("FD_Interest_Rate") / 1200, F.col("FD_Tenure")), 2))

    .withColumn("FD_Monthly_Maturity_Amount", round(F.col("FD_Maturity_Amount") / F.col("FD_Tenure"), 2))

    .withColumn("FD_Status", F.when(F.col("FD_Maturity_Date") > F.current_date(), "Active").otherwise("Closed"))
)

cal_FD_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Display FD_Status**

# COMMAND ----------

fd_status = cal_FD_df.select(
    "FD_Type",
    "FD_Amount",
    "FD_Deposit_Date",
    "FD_Tenure",
    "FD_Interest_Rate",
    "FD_Tenure_Classification",
    "FD_Maturity_Date",
    "FD_Maturity_Amount",
    "FD_Monthly_Maturity_Amount",
    "FD_Status"
)
fd_status.display()

# COMMAND ----------

cal_FD_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Create one column called Maturity Amount Category add condition like
# MAGIC i) if Maturity amount is < 250000 = "Low"
# MAGIC ii) Maturity amount >=250000 & maturity amount <= 810000  = "Medium"
# MAGIC iii) otherwise = "High"**

# COMMAND ----------

cal_FD_df = cal_FD_df.withColumn("FD_Maturity_Amount_Category",
                                    when(col("FD_Maturity_Amount") < 250000, "Low")
                                    .when((col("FD_Maturity_Amount") >= 250000) & (col("FD_Maturity_Amount") < 810000),
                                           "Medium")
                                     .otherwise("High")
                                     )
cal_FD_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Calculate The Total and Agreegate of Maturity Amount by Maturity Amount Category.**

# COMMAND ----------

cal_tot_agg_FD = cal_FD_df.groupBy("FD_Maturity_Amount_Category").agg(
    round(sum("FD_Maturity_Amount"), 2).alias("FD_Total_Maturity_Amount"),
    round(avg("FD_Maturity_Amount"), 2).alias("FD_Average_Maturity_Amount")
)
cal_tot_agg_FD.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Display cal_FD_df dataframe**

# COMMAND ----------

cal_FD_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **write data into the gold layer and create a folder called FD_details and write FD related data write into FD_Details folder in parquet files**

# COMMAND ----------

from datetime import datetime
from datetime import date
current_date = datetime.now().strftime("%Y-%m-%d")

folder_path = f"mnt/gold/{current_date}/FD_Details"
cal_FD_df.write.mode("overwrite").parquet(folder_path)

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformations On Loan Columns**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **All Branch data combined into one dataframe and display that dataframe**
# MAGIC **And Printing Schema of that dataframe**

# COMMAND ----------

union_df.display()
union_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Loan Application Approved By Bank**

# COMMAND ----------

approved_loans_df = union_df.filter(F.col("Loan_Application_Status") == "Approved")
approved_loans_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Branch wise Approved loan count**

# COMMAND ----------

branch_loan_count_df = union_df.groupBy("Branch").agg(F.count("*").alias("Approved_Loan_Count"))
branch_loan_count_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Yearwise and Branchwise Total loan amount by bank**

# COMMAND ----------

from pyspark.sql.functions import col, year, count, sum

year_branch_loan_df = union_df.withColumn("Loan_Year", year(col("Loan_Start_Date")))

result_df1 = year_branch_loan_df.groupBy("Loan_year", "Branch").agg(
    sum("Loan_Amount_Requested").alias("Total_Loan_Amount")
)
result_df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Calculate loan_income ratio**

# COMMAND ----------

income_ratio_df = union_df.withColumn("Loan_To_Income_Ratio", round(col("Loan_Amount_Requested") / col("Annual_Income"), 3))
income_ratio_df.display()

# COMMAND ----------

income_ratio_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Calculate the Loan_Maturity_Date, Loan_Maturity_Amount, Monthly_Loan_Repayment_Amount and Loan_Status from particular column like Loan Amount Requested, Loan_Interest_Rate_Offered, Loan_Start_Date and Loan Term.**

# COMMAND ----------

from pyspark.sql.functions import col, expr, when, current_date

# Loan Maturity Date
cal_Loan_df= (income_ratio_df.withColumn("Loan_Maturity_Date", expr("add_months(Loan_Start_Date, Loan_Term)"))
              
            .withColumn("Loan_Maturity_Amount", 
                            round(col("Loan_Amount_Requested") * (1 + col("Loan_Interest_Rate_Offered") / 100),2))
            
            .withColumn("Monthly_Loan_Interest_Rate", round(col("Loan_Interest_Rate_Offered") / 12 / 100,2))

            .withColumn("Monthly_Loan_Repayment_Amount",
                            when(col("Loan_Interest_Rate_Offered") == 0, 
                            col("Loan_Amount_Requested") / col("Loan_Term"))
                            .otherwise(
                            (col("Loan_Amount_Requested") * col("Monthly_Loan_Interest_Rate") *
                            (1 + col("Monthly_Loan_Interest_Rate")) ** col("Loan_Term")) /
                            ((1 + col("Monthly_Loan_Interest_Rate")) ** col("Loan_Term") - 1)
                            ))

            .withColumn("Loan_Status",
                   when(col("Loan_Maturity_Date") > current_date(), "Active")
                   .otherwise("Completed")))
cal_Loan_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Loan Status display**

# COMMAND ----------

loan_status = cal_Loan_df.select(
    "Loan_Type",
    "Loan_Amount_Requested",
    "Loan_Start_Date",
    "Loan_Term",
    "Loan_Interest_Rate_Offered",
    "Loan_Maturity_Date",
    "Loan_Maturity_Amount",
    "Monthly_Loan_Interest_Rate",
    "Monthly_Loan_Repayment_Amount",
    "Loan_Status"
)
loan_status.display()

# COMMAND ----------

cal_Loan_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **write data into the gold layer and create a folder called loan_details and write loan related data write into Loan_Details folder in parquet files**

# COMMAND ----------

from datetime import datetime
from datetime import date
current_date = datetime.now().strftime("%Y-%m-%d")

folder_path = f"mnt/gold/{current_date}/Loan_Details"
cal_Loan_df.write.mode("overwrite").parquet(folder_path)