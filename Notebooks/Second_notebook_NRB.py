# Databricks notebook source
dbutils.fs.ls("/mnt/silver/customers")

# COMMAND ----------

dbutils.fs.ls("/mnt/silver/accounts")

# COMMAND ----------

path='dbfs:/mnt/silver/customers/part-00000-tid-6655138705804169634-48adb659-4425-4d54-9ca0-18d9f5252f94-5-1.c000.snappy.parquet'
df1=spark.read.parquet(path,header=True,inferSchema=True)
df1.display()
path1='dbfs:/mnt/silver/accounts/part-00000-892f5960-0615-40ea-9d69-bfca30e95e8a.c000.snappy.parquet'
df2=spark.read.parquet(path1,header=True,inferSchema=True)
df2.display()

# COMMAND ----------

cust_accounts = df1.alias("c").join(df2.alias("a"), df1.customer_id == df2.customer_id, "inner") \
    .select(
        df1.customer_id,
        df1.dob,
        df1.email,
        df1.gender,
        df1.is_active,
        df1.name,
        df1.region,
        df2.account_id,
        df2.account_type,
        df2.balance,
        df2.branch_code,
        df2.opened_on
    )
cust_accounts.display()

# COMMAND ----------

len(cust_accounts.columns)

# COMMAND ----------

high_salary=cust_accounts.filter(cust_accounts.balance>10000)
high_salary.display()

# COMMAND ----------

active_customers=cust_accounts.filter(cust_accounts.is_active=='Active')
active_customers.display()

# COMMAND ----------

len(active_customers.columns)

# COMMAND ----------

df_bal=active_customers.groupBy('account_type').agg({'balance':'avg'})
df_bal.display()

# COMMAND ----------

from pyspark.sql.functions import row_number,col,dense_rank
from pyspark.sql.window import Window

ranked_df = active_customers.withColumn("rank", dense_rank().over(Window.orderBy(col("balance").desc())))
display(ranked_df)

# COMMAND ----------

from pyspark.sql.functions import row_number,col,dense_rank,rank
from pyspark.sql.window import Window
#ranked1_df = active_customers.withColumn("rank", row_number().over(Window.orderBy(col("balance").desc())))

ranked1_df = active_customers.withColumn("rank", rank().over(Window.orderBy(col("balance").desc())))
display(ranked1_df)

# COMMAND ----------

filtered_customers = ranked1_df.filter(ranked1_df.opened_on < '2023-01-01') \
    .select('name', 'account_id',"opened_on")

display(filtered_customers)

# COMMAND ----------

dbutils.fs.ls("/mnt/silver/transactions")

# COMMAND ----------

path2='dbfs:/mnt/silver/transactions/part-00000-aa49675c-5ddd-42b3-8a48-c8d4da305407.c000.snappy.parquet'
transactions=spark.read.parquet(path2,header=True,inferSchema=True)
transactions.display()

# COMMAND ----------

transact=transactions.withColumn("transactions_account_id",transactions.account_id)\
    .drop("account_id")
transact.display()

# COMMAND ----------

cust_transactions=active_customers.join(transact,active_customers.account_id==transact.transactions_account_id,"inner")
cust_transactions.display()

# COMMAND ----------

len(cust_transactions.columns)

# COMMAND ----------

cust_transactions.printSchema()

# COMMAND ----------

cust_transactions1=cust_transactions.drop("transactions_account_id")
display(cust_transactions1)

# COMMAND ----------

len(cust_transactions1.columns)

# COMMAND ----------

dbutils.fs.ls("/mnt/silver/branches")

# COMMAND ----------

path3='dbfs:/mnt/silver/branches/part-00000-e9032f1a-44c7-42ff-ab2c-ec76aaf456c4.c000.snappy.parquet'
branches=spark.read.parquet(path3,header=True,inferSchema=True)
branches.display()

# COMMAND ----------

#if there is column ambiguity means same column with same values coming twice then we can create
#new column from it or we can rename it and finally drop it if it is already present in dataframe
from pyspark.sql.functions import col

branch1 = branches.withColumn("bank_branch_code", col("branch_code"))\
    .withColumn("bank_region",col("region"))\
    .drop("branch_code","region")
display(branch1)

# COMMAND ----------

df3 = cust_transactions1.join(branch1, cust_transactions.branch_code == branch1.bank_branch_code, 'inner')\
    .withColumnRenamed("amount","transaction_amount")
display(df3)

# COMMAND ----------

df3.printSchema()

# COMMAND ----------

len(df3.columns)

# COMMAND ----------

df4=df3.drop("bank_branch_code","bank_region")
display(df4)
len(df4.columns)

# COMMAND ----------

dbutils.fs.ls("/mnt/silver/loans")

# COMMAND ----------

path4='dbfs:/mnt/silver/loans/part-00000-6f09b867-0cfa-4d22-aec9-5ed6bddfac18.c000.snappy.parquet'
df5=spark.read.parquet(path4,header=True,inferSchema=True)
display(df5)

# COMMAND ----------

loan1 = df5.filter((df5.status == "Active") | (df5.status == "Defaulted"))\
    .withColumnRenamed("amount","loan_amount")
loan1.display()

# COMMAND ----------

from pyspark.sql.functions import col

loan2 = loan1.withColumnRenamed("customer_id", "cust_id")
display(loan2)

# COMMAND ----------

cust_loan=df4.join(loan2,df4.customer_id==loan2.cust_id,"inner")\
    .drop("cust_id")
cust_loan.display()

# COMMAND ----------

len(cust_loan.columns)

# COMMAND ----------

dbutils.fs.ls("/mnt/silver/feedback")

# COMMAND ----------

path5='dbfs:/mnt/silver/feedback/part-00000-d771134b-0436-4bd6-bc2e-884d8c21abe9.c000.snappy.parquet'
df6=spark.read.parquet(path5,header=True,inferSchema=True)
display(df6)

# COMMAND ----------

feed1=df6.withColumnRenamed("customer_id","cust1_id")
feed1.display()

# COMMAND ----------

cust_feed=cust_loan.join(feed1,cust_loan.customer_id==feed1.cust1_id,"inner")\
    .drop("cust1_id")
cust_feed.display()

# COMMAND ----------

len(cust_feed.columns)

# COMMAND ----------

dbutils.fs.ls("/mnt/silver/credit_scores")

# COMMAND ----------

path6='dbfs:/mnt/silver/credit_scores/part-00000-efce572c-99d4-4187-8e38-cfba850e78d0.c000.snappy.parquet'
df7=spark.read.parquet(path6,header=True,inferSchema=True)
display(df7)

# COMMAND ----------

from pyspark.sql.functions import col
df8=df7.withColumn("cust2_id",col("customer_id"))\
    .drop("customer_id")
df8.display()

# COMMAND ----------

df9=cust_feed.join(df8,cust_feed.customer_id==df8.cust2_id,"inner")\
    .drop("cust2_id")
df9.display()

# COMMAND ----------

len(df9.columns)

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/silver/activity_log")

# COMMAND ----------

path7='dbfs:/mnt/silver/activity_log/part-00000-d144c097-88e0-4723-8165-a638174f46ff.c000.snappy.parquet'
df10=spark.read.parquet(path7,header=True,inferSchema=True)
df10.display()

# COMMAND ----------

df11=df10.withColumnRenamed("customer_id","cust3_id")
df11.display()

# COMMAND ----------

df12=df9.join(df11,df9.customer_id==df11.cust3_id,"inner")\
    .drop("cust3_id")
display(df12)
len(df12.columns)

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/silver/products")

# COMMAND ----------

path8='dbfs:/mnt/silver/products/part-00000-e176f20e-46f7-4306-a015-c79edb53655a.c000.snappy.parquet'
bank_product1=spark.read.parquet(path8,header=True,inferSchema=True)
bank_product1.display()

# COMMAND ----------

df14=df12.select("customer_id").distinct()
bank_prod1 = bank_product1.crossJoin(df14).limit(25)
display(bank_prod1)

# COMMAND ----------

bank_prod2=bank_prod1.withColumnRenamed("customer_id","cust5_id")\
    .drop("account_type")
bank_prod2.display()
final_customer_df1=df12.join(bank_prod2,df12.customer_id==bank_prod2.cust5_id,"inner")\
    .drop("cust5_id")
final_customer_df1.display()

# COMMAND ----------

len(final_customer_df1.columns)

# COMMAND ----------

final_customer_df1.coalesce(1).write.format("parquet").mode("overwrite").option("header", "true").save("dbfs:/mnt/gold/final_customer_data")

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/silver/employees")

# COMMAND ----------

