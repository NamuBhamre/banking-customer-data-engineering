# Databricks notebook source
dbutils.fs.ls("/mnt/silver/employees")

# COMMAND ----------

path='dbfs:/mnt/silver/employees/part-00000-2324c3dd-112a-40f5-be37-29665fc83a80.c000.snappy.parquet'
df = spark.read.parquet(path, header=True, inferSchema=True)
display(df)

# COMMAND ----------

'''roles = df.select("role").distinct().collect()
for role in roles:
    role_name = role['role']
    df_role = df.filter(df.role == role_name)
    display(df_role)'''

df2=df.filter(df.role == "Branch Manager")\
   # .select("employee_id", "name", "role")
display(df2)

# COMMAND ----------

df3=df.filter((df.employee_id == "E5011") & (df.branch_code == "BR003"))\
    .select("employee_id", "name", "role")
display(df3)

# COMMAND ----------

dbutils.fs.ls("/mnt/silver/branches")

# COMMAND ----------

path1='dbfs:/mnt/silver/branches/part-00000-e9032f1a-44c7-42ff-ab2c-ec76aaf456c4.c000.snappy.parquet'
df1 = spark.read.parquet(path1, header=True, inferSchema=True)
display(df1)

# COMMAND ----------

from pyspark.sql.functions import *
branch1 = df1.withColumn("bank_branch_code", col("branch_code"))\
    .drop("branch_code")
display(branch1)

# COMMAND ----------

df4=df.join(branch1, df.branch_code == branch1.bank_branch_code)\
    .drop("bank_branch_code")
display(df4)


# COMMAND ----------

df5 = df4.filter((df4.region == "North") & (df4.role == "Loan Officer"))\
    .select("employee_id", "name", "role")
display(df5)

# COMMAND ----------

df4.coalesce(1).write.format("parquet").mode("overwrite").option("header", "true").save("dbfs:/mnt/gold/final_employee_data")