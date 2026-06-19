# Databricks notebook source
dbutils.fs.ls("/mnt/bronze/dbo")

# COMMAND ----------

path="dbfs:/mnt/bronze/dbo/customers/"
df1=spark.read.csv(path,header=True,inferSchema=True)
display(df1)

# COMMAND ----------

df1.printSchema()


# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

# Identify string columns
string_cols = [field.name for field in df1.schema.fields if field.dataType.simpleString() == 'string']

# Apply transformation to remove double quotes
df_cust = df1.select([
    regexp_replace(col(c), '"', '').alias(c) if c in string_cols else col(c)
    for c in df1.columns
])

df_cust.display()



# COMMAND ----------

from pyspark.sql.functions import when

cust1_df1 = df_cust.withColumn(
    "is_active",
    when(df_cust.is_active == True, "Active").otherwise("Inactive")
)
display(cust1_df1)


# COMMAND ----------

cust1_df1.write.format("parquet").mode("overwrite").option("header", "true").save("/mnt/silver/customers/")

# COMMAND ----------

path1="dbfs:/mnt/bronze/dbo/accounts/"
df2=spark.read.csv(path1,header=True,inferSchema=True)
display(df2)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

# Identify string columns
string_cols = [field.name for field in df2.schema.fields if field.dataType.simpleString() == 'string']

# Apply transformation to remove double quotes
df_acc = df2.select([
    regexp_replace(col(c), '"', '').alias(c) if c in string_cols else col(c)
    for c in df2.columns
])

df_acc.display()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, StringType

# Create a new DataFrame with 'balance' cast to double
acc1_df1 = df_acc.select([
    col("balance").cast(DoubleType()).alias("balance") if c == "balance" else col(c).cast(StringType()).alias(c)
    for c in df_acc.columns
])
acc1_df1.display()


# COMMAND ----------

from pyspark.sql.functions import to_date, col

# Assuming the date format in your string column is 'yyyy-MM-dd'
acc1_df2 = acc1_df1.withColumn("opened_on", to_date(col("opened_on"), "yyyy-MM-dd"))
acc1_df2.display()


# COMMAND ----------

#spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
#spark.conf.set("parquet.enable.summary-metadata", "True")
#spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "True")


# COMMAND ----------

acc1_df2.write.format("parquet").mode("overwrite").save("/mnt/silver/accounts/")

# COMMAND ----------

path3='dbfs:/mnt/bronze/dbo/activity_log/'
df3=spark.read.csv(path3,header=True,inferSchema=True)
display(df3)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

# Identify string columns
string_cols = [field.name for field in df3.schema.fields if field.dataType.simpleString() == 'string']

# Apply transformation to remove double quotes
df4 = df3.select([
    regexp_replace(col(c), '"', '').alias(c) if c in string_cols else col(c)
    for c in df3.columns
])

df4.display()

# COMMAND ----------

df4.write.format("parquet").mode("overwrite").option("header", "true").save("/mnt/silver/activity_log")

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze/dbo")

# COMMAND ----------

path5='dbfs:/mnt/bronze/dbo/credit_scores/'
df5=spark.read.csv(path5,header=True,inferSchema=True)
df5.display()

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

# Identify string columns
string_cols = [field.name for field in df5.schema.fields if field.dataType.simpleString() == 'string']

# Apply transformation to remove double quotes
df6 = df5.select([
    regexp_replace(col(c), '"', '').alias(c) if c in string_cols else col(c)
    for c in df5.columns
])

df6.display()

# COMMAND ----------

df6.write.format("parquet").mode("overwrite").option("header", "true").save("/mnt/silver/credit_scores")

# COMMAND ----------

path7='dbfs:/mnt/bronze/dbo/feedback/'
df7=spark.read.csv(path7,header=True,inferSchema=True)
display(df7)

# COMMAND ----------

df7.write.format("parquet").mode("overwrite").option("header", "true").save("/mnt/silver/feedback")

# COMMAND ----------

path8='dbfs:/mnt/bronze/dbo/loans/'
df8=spark.read.csv(path8,header=True,inferSchema=True)
display(df8)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

# Identify string columns
string_cols = [field.name for field in df8.schema.fields if field.dataType.simpleString() == 'string']

# Apply transformation to remove double quotes
df9 = df8.select([
    regexp_replace(col(c), '"', '').alias(c) if c in string_cols else col(c)
    for c in df8.columns
])

df9.display()

# COMMAND ----------

df10=df9.withColumn("amount",col("amount").cast("double"))
df10.display()

# COMMAND ----------

df10.write.format("parquet").mode("overwrite").option("header", "true").save("/mnt/silver/loans")

# COMMAND ----------

path9='dbfs:/mnt/bronze/dbo/products/'
df11=spark.read.csv(path9,header=True,inferSchema=True)
display(df11)


# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

# Identify string columns
string_cols = [field.name for field in df11.schema.fields if field.dataType.simpleString() == 'string']

# Apply transformation to remove double quotes
df12 = df11.select([
    regexp_replace(col(c), '"', '').alias(c) if c in string_cols else col(c)
    for c in df11.columns
])

df12.display()

# COMMAND ----------

from pyspark.sql.functions import concat_ws, when

df13 = df12.withColumn(
    "feature",
    when(col("features__003") == "", concat_ws(",", col("features__001"), col("features__002")))
    .otherwise(concat_ws(",", col("features__001"), col("features__002"), col("features__003")))
).drop("features__001", "features__002", "features__003")
df13.display()

# COMMAND ----------

df13.write.format("parquet").mode("overwrite").option("header", "true").save("/mnt/silver/products")

# COMMAND ----------

path10='dbfs:/mnt/bronze/dbo/transactions/'
df14=spark.read.csv(path10,header=True,inferSchema=True)
display(df14)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

# Identify string columns
string_cols = [field.name for field in df14.schema.fields if field.dataType.simpleString() == 'string']

# Apply transformation to remove double quotes
df15 = df14.select([
    regexp_replace(col(c), '"', '').alias(c) if c in string_cols else col(c)
    for c in df14.columns
])

df15.display()

# COMMAND ----------

df16=df15.withColumn("amount",col("amount").cast("double"))
df16.display()

# COMMAND ----------

df16.write.format("parquet").mode("overwrite").option("header", "true").save("/mnt/silver/transactions")

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze/dbo")

# COMMAND ----------

path11='dbfs:/mnt/bronze/dbo/employees/'
df17=spark.read.csv(path11,header=True,inferSchema=True)
display(df17)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

# Identify string columns
string_cols = [field.name for field in df17.schema.fields if field.dataType.simpleString() == 'string']

# Apply transformation to remove double quotes
df18 = df17.select([
    regexp_replace(col(c), '"', '').alias(c) if c in string_cols else col(c)
    for c in df17.columns
])

df18.display()

# COMMAND ----------

df18.write.format("parquet").mode("overwrite").option("header", "true").save("/mnt/silver/employees")

# COMMAND ----------

path12='dbfs:/mnt/bronze/dbo/branches/'
df19=spark.read.csv(path12,header=True,inferSchema=True)
display(df19)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

# Identify string columns
string_cols = [field.name for field in df19.schema.fields if field.dataType.simpleString() == 'string']

# Apply transformation to remove double quotes
df20 = df19.select([
    regexp_replace(col(c), '"', '').alias(c) if c in string_cols else col(c)
    for c in df19.columns
])

display(df20)

# COMMAND ----------

df20.write.format("parquet").mode("overwrite").option("header", "true").save("/mnt/silver/branches")