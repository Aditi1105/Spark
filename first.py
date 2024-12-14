import os
import urllib.request
import ssl
from inspect import FullArgSpec

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt": os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe": os.path.join(data_dir1, "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll": os.path.join(data_dir1, "hadoop.dll")
}

# Create an unverified SSL context
ssl_context = ssl._create_unverified_context()

for url, path in urls_and_paths.items():
    # Use the unverified context with urlopen
    with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
        data = response.read()
        out_file.write(data)
import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://github.com/saiadityaus1/test1/raw/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv"}.items()]

# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['JAVA_HOME'] = r'C:\Users\Shuvait\.jdks\corretto-1.8.0_432'

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)
##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################








#print("=======zeyo started=======")
#a = 2
#print(a)

#b = 3
#print(b)

#c = a+2
#print(c)

#d = "Zeyobron"

#print(d)

#e = d + " Analytics"
#print(e)

#print()
#print(" INT ITERATIONS")
#ln = [1,2,3,4,5]
#print()
#print("=====RAW LIST=====")
#print()
#print(ln)

#rddln = sc.parallelize(ln)                    ##convert to RDD to process
#print()
#print("=====RDD LIST=====")
#print()
#print(rddln.collect())


#addrdd = rddln.map(lambda x : x+2)
#print("=====ADD 2 RDD LIST======")
#print(addrdd.collect())

#mulrdd = rddln.map(lambda x : x * 10)
#print("=====MULTIPLY 10 RDD LIST=====")
#print(mulrdd.collect())

#addrdd = rddln.map(lambda x : x + 5)
#()
#print("=====ADD 5 RDD LIST======")
#print()
#print(addrdd.collect())

#mulrdd = rddln.map(lambda x : x * 5)
#print()
#print("=====MULTIPLY 5 RDD LIST=====")
#print()
#print(mulrdd.collect())

#divrdd = rddln.map(lambda x : x / 5)
#print()
#print("=====DIVIDE 5 RDD LIST=====")
#print()
#print(divrdd.collect())


#filrdd = rddln.filter( lambda x : x > 2 )     ## Filter operation
#print()
#print("=====Filter RDD =====")
#print()
#print(filrdd.collect())

### STRING ###
###print(" STRING ITERATIONS")

###listr = ["zeyobron", "zeyo","analytics"]


###rddstr = sc.parallelize(listr)
###print()
###print("=====String RDD=====")
###print()
###print(rddstr.collect())
###print()

###constr = rddstr.map(lambda x : x + " analytics")
###print("=====Concate String=====")
###print()
###print(constr.collect())
###print()


###repstr = rddstr.map(lambda x : x.replace("zeyo", "tera"))
###print("=====Replace String=====")
###print()
###print(repstr.collect())
###print()

###filstr = rddstr.filter(lambda x : "zeyo" in x )
###print("=====Filter String=====")
###print()
###print(filstr.collect())
###print()




#print(" FLATTEN  ITERATION")

#strf = ["A~B", "C~D"]
#print()

#rddf = sc.parallelize(strf)
#print()
#print("=====RDD LIST=====")
#print(rddf.collect())
#print()

#flatrdd = rddf.flatMap(lambda x : x.split("~"))
#print()
#print("=====FLATTEN LIST=====")
#print(flatrdd.collect())
#print()






## when two delimiter
#liststr = ["A~B,C", "D~E,F"]

#rddstr = sc.parallelize(liststr)
#print()
#print("=====RDD LIST 2=====")
#print(rddstr.collect())
#print()

#flatstr = rddstr.flatMap(lambda x : x.split("~")).flatMap(lambda x : x.split(","))
#print()
#print("=====FLATTEN LIST 2=====")
#print(flatstr.collect())
#print()



#Raw usecase data

# data = [
#     "State->TN~City->Chennai",
#     "State->UP~City->Lucknow"
# ]
# rdds = sc.parallelize(data)
#
# print("===============RAW DATA=============")
# rdds.foreach(print)
# print()
#
#
# flatdata = rdds.flatMap(lambda x: x.split("~"))
# print("===============Flat DATA=============")
# flatdata.foreach(print)
# print()
#
# states = flatdata.filter(lambda x: "State" in x)
# print("===============States DATA=============")
# states.foreach(print)
# print()
#
# finalstates = states.map(lambda x: x.replace("State->", ""))
# print("===============Final States DATA=============")
# finalstates.foreach(print)
# print()
#
#
# cities = flatdata.filter(lambda x: "City" in x)
# print("===============Cities DATA=============")
# cities.foreach(print)
# print()
#
# finalcities = cities.map(lambda x: x.replace("City->", ""))
# print("===============Final City DATA=============")
# finalcities.foreach(print)
# print()




#Task Question

# data = [
#     "bigdata~spark~hadoop~hive"
# ]
#
# newrdd = sc.parallelize(data)
#
# print("===============RAW DATA=============")
# print(newrdd.collect())
# print()
#
#
# finalrdd = newrdd.flatMap(lambda x: x.split("~")).map(lambda x : x.upper()).map(lambda x: "Tech-> " + x + " TRAINER->SAI")
# print("===============finalrdd DATA=============")
# finalrdd.foreach(print)                  ## to print data in each separate line
#


## Text file code

# rdds = sc.textFile("state.txt")
#
# print("===============RAW DATA=============")
# rdds.foreach(print)
# print()
#
#
# flatdata = rdds.flatMap(lambda x: x.split(","))
# print("===============Flat DATA=============")
# flatdata.foreach(print)
# print()
#
# states = flatdata.filter(lambda x: "State" in x)
# print("===============States DATA=============")
# states.foreach(print)
# print()
#
# finalstates = states.map(lambda x: x.replace("State->", ""))
# print("===============Final States DATA=============")
# finalstates.foreach(print)
# print()
#
#
# cities = flatdata.filter(lambda x: "City" in x)
# print("===============Cities DATA=============")
# cities.foreach(print)
# print()
#
# finalcities = cities.map(lambda x: x.replace("City->", ""))
# print("===============Final City DATA=============")
# finalcities.foreach(print)
# print()

#read textfile usdata.csv
# Read the data
# Filter the lines who length > 200
# Flatten the result with comma
# Replace the flatten data "-" WITH EMPTY (REMOVE IT)
# Concat  ,zeyo for the result
# Write the results to a file


# data = sc.textFile("usdata.csv")
# print("=====RAW DATA=====")
# data.foreach(print)
# print()
#
#
# fildata = data.filter(lambda x : len(x) > 200)
# print()
# print("=====FILTER DATA of LENGTH > 200======")
# fildata.foreach(print)
# print()
#
#
# flatdata = fildata.flatMap(lambda x : x.split(","))
# print()
# print("=====FLAT DATA======")
# flatdata.foreach(print)
# print()
#
#
# repdata = flatdata.map(lambda x : x.replace("-",""))
# print()
# print("=====REPLACE '-' DATA======")
# repdata.foreach(print)
# print()
#
# condata = repdata.map(lambda x : x + ",zeyo")
# print()
# print("=====CONCAT DATA======")
# condata.foreach(print)
# print()





###COLUMN BASED PROCESSING

# print("=====STARTED======")
# data = sc.textFile("dt.txt")
# print("====RAW DATA=====")
# print()
# data.foreach(print)
# print()
#
# mapsplit = data.map(lambda x : x.split(","))
# print("====mapsplit DATA=====")
# print()
# mapsplit.foreach(print)
# print()
#
# from collections import namedtuple
#
# columns = namedtuple("columns",["id","tno","amt","category","product","mode"])
#
# assigncol = mapsplit.map(lambda x : columns(x[0],x[1],x[2],x[3],x[4],x[5]))
#
#
# prodfilter = assigncol.filter(lambda x : "Gymnastics" in x.product)
#
# print("====prodfilter DATA=====")
# print()
# prodfilter.foreach(print)
# print()
#
# df=prodfilter.toDF()
# df.show()
#
# df.createOrReplaceTempView("trump")
#
# spark.sql("select id,tno from trump;").show()



## Different file reads

# csvdf = (
#          spark
#          .read
#          .format("csv")
#          .option("header","true")
#          .load("df.csv")
# )
# print("CSV FILE TO DATAFRAME")
# print()
# csvdf.show()
#
# jsondf = (
#     spark
#     .read
#     .format("json")
#     .load("file4.json")
# )
# print("JSON FILE TO DATAFRAME")
# print()
# jsondf.show()
#
#
# parquetdf = (
#     spark
#     .read
#     .format("parquet")
#     .load("file5.parquet")
# )
# print("PARQUET FILE TO DATAFRAME")
# print()
# parquetdf.show()
#
#
# orcdf = (
#     spark
#     .read
#     .format("orc")
#     .load("data.orc")
# )
# print("ORC FILE TO DATAFRAME")
# print()
# orcdf.show()



##  DSL

# data = [
#     ("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
#     ("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
#     ("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
#     ("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
#     ("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
#     ("00000005", "02-14-2011", 200, "Gymnastics", None, "cash")
# ]
#
#
# df = spark.createDataFrame(data).toDF("tno", "tdate", "amount", "category", "product", "spendby")
# print("=== ALL DATA ===")
# df.show()

# seldf=df.select("tno","tdate")
# seldf.show()
#
#
# dropdf=df.drop("tno","tdate")
# dropdf.show()
#
#
#
# # single column filter
#
# singcol=df.filter("category = 'Exercise'")
# print(" === 1. SINGLE COLUMN FILTER === ")
# singcol.show()
#
#
# ## Multi column AND filter
#
# multicol1=df.filter("category = 'Exercise' and spendby = 'cash' ")
# print(" === 2. MULTI COLUMN AND FILTER === ")
# multicol1.show()
#
#
# ## Multi column OR filter
#
# multicol2=df.filter("category = 'Exercise' or spendby = 'cash' ")
# print(" === 3. MULTI COLUMN OR FILTER === ")
# multicol2.show()
#
# ## Multi VALUE IN filter
#
# multival=df.filter("category in ('Exercise' , 'Gymnastics') ")
# print(" === 4. MULTI VALUE IN FILTER === ")
# multival.show()
#
#
# ## NULL VALUE filter
#
# nullval=df.filter("product is NULL ")
# print(" === 5. NULL FILTER === ")
# nullval.show()
#
#
# ## NOT NULL VALUE filter
#
# nullval=df.filter("product is not NULL ")
# print(" === 6. NOT NULL FILTER === ")
# nullval.show()
#
#
# ##Like operator
#
# prodfilter = df.filter("product like '%Gymnastics%'")
# print(" === 7. LIKE FILTER === ")
# prodfilter.show()
#
#
#
# ## != operator
#
# notfilter = df.filter("category != 'Exercise' ")
# print(" === 8. NOT FILTER === ")
# notfilter.show()


### EXPRESSIONS
from pyspark.sql.functions import *  #🔴🔴


# categdf = df.selectExpr(
#
#     "tno",  #Column
#     "tdate",   #Column
#     "amount", #Column
#     "  upper(category) as category ",  # Expression
#     "product", #Column
#     "spendby" #Column
#
# )
# print(" === Upper case category ===")
# categdf.show()
#
#
# proddf = df.selectExpr(
#
#     "tno",  #Column
#     "tdate",   #Column
#     "amount", #Column
#     "category",  # Expression
#     " lower (product) as product ", #Column
#     "spendby" #Column
#
# )
# print(" === lower case product ===")
# proddf.show()
#
#
#
# tnodf = df.selectExpr(
#
#
#       "cast(tno as int) tno",
#       "tdate",   #Column
#       "amount", #Column
#       "category",  # Expression
#       "product ", #Column
#       "spendby" #Column
# )
# print(" === Cast Tno as Int ===")
# tnodf.show()


# import time
# time.sleep(400)   #  in browser type  localhost:4040



# result = df.filter("category = 'Exercise'").select("tno", "spendby")
# result.show()

## EXPRESSIONS
# procdf =  df.selectExpr(
#           "cast (tno as int) as tno",
#           "split(tdate, '-')[2] as tdate ",
#           "amount + 100 as amount ",
#           "upper(category) as category ",
#           "concat(product,'~zeyo') as product ",
#           "spendby",
#           """
#           case
#           when spendby = 'cash' then 0
#           when spendby = 'paytm' then 2
#           else 1
#           end
#           as status """
# )
# print("=== All Expressions ===")
# procdf.show()


# from pyspark.sql.functions import *
#
# withcolexpr = (
#                df.withColumn("tno", expr ("cast(tno as int)" ))
#                  .withColumn("tdate",expr ("split(tdate, '-') [2] "))
#                  .withColumn("product", expr ("concat(product, '~zeyo') "))
#                  .withColumn("amount", expr ("amount +100" ))
#                  .withColumn("category", expr ("upper (category)" ))
#                  .withColumn("status", expr ("case when spendby = 'cash' then 0 when spendby = 'paytm' then 2 else 1 end" ))
#                  .withColumnRenamed("tdate", "year")
# )
# print(" === WITH COLUMN === ")
# withcolexpr.show()
#


#### UNION ####

# Task 1
#
# data = [("1",)]
# df1 =  spark.createDataFrame(data, ["col1"])
# df1.show()
#
# data1 = [("2",)]
#
# df2 =  spark.createDataFrame(data1, ["col2"])
# df2.show()
#
#
# uniondf = df1.union(df2).withColumnRenamed("col1","value")
# print(" === Task 1 UNION === ")
# uniondf.show()












# # TASK 2
# # 🔴  DATA PREPARATION
#
# data = [("m1", "m1,m2", "m1,m2,m3", "m1,m2,m3,m4")]
#
# df = spark.createDataFrame(data, ["col1", "col2", "col3", "col4"])
# print(" === Task 2 RAW DATA === ")
# df.show()
#
# df1= df.select("col1")
# df1.show()
# df2= df.select("col2")
# df2.show()
# df3= df.select("col3")
# df3.show()
# df4= df.select("col4")
# df4.show()
#
# uniondf = df1.union(df2).union(df3).union(df4).withColumnRenamed("col1","col")
# print(" === Task 2 UNION === ")
# uniondf.show()

#
# #👉 Preparation Code
#
# data = [
#     (203040, "rajesh", 10, 20, 30, 40, 50)
# ]
#
# df = spark.createDataFrame(data, ["rollno", "name", "telugu", "english", "maths", "science", "social"])
# df.show()
#
#
# sumdf = df.withColumn("total",expr("telugu +english +maths +science +social"))
#
# sumdf.show()
#

##JOINS
##There are 7 types of joins.
# 1.Inner Join
# 2. Left
# 3. Right
# 4. Full
# 5. Left Anti
# 6. Left Semi
# 7. Cross


# data4 = [
#     (1, "raj"),
#     (2, "ravi"),
#     (3, "sai"),
#     (5, "rani")
# ]
#
# df1 = spark.createDataFrame(data4, ["id", "name"])
# df1.show()
#
# data3 = [
#     (1, "mouse"),
#     (3, "mobile"),
#     (7, "laptop")
# ]
# df2 = spark.createDataFrame(data3, ["id1", "product"])
# df2.show()

# print("==== 1. INNER JOIN ====")
#
# innerjoin = df1.join(df2, ["id"],"inner")
# innerjoin.show()
#
# print("==== 2. LEFT JOIN ====")
#
# leftjoin = df1.join(df2, ["id"],"left").orderBy("id")
# leftjoin.show()
#
# print("==== 3. RIGHT JOIN ====")
#
# rightjoin = df1.join(df2, ["id"],"right").orderBy("id")
# rightjoin.show()
#
# print("==== 4. FULL JOIN ====")
#
# fulljoin = df1.join(df2, ["id"],"full").orderBy("id")
# fulljoin.show()

#
# print("==== DIFFERENT COLUMN NAME FOR FULL JOIN ====")
#
# innerjoin = df1.join(df2, df1["id"] == df2 ["id1"],"inner").orderBy("id").drop("id1")
# innerjoin.show()
#
# print("==== DIFFERENT COLUMN NAME FOR LEFT JOIN ====")
#
# leftjoin = df1.join(df2, df1["id"] == df2 ["id1"],"left").orderBy("id").drop("id1")
# leftjoin.show()
#
# print("==== DIFFERENT COLUMN NAME FOR RIGHT JOIN ====")
#
# leftjoin = df1.join(df2, df1["id"] == df2 ["id1"],"right").orderBy("id1").drop("id")
# leftjoin.show()
#
# print("==== DIFFERENT COLUMN NAME FOR FULL JOIN ====")
#
# fulljoin = (
#                df1.join(df2, df1["id"] == df2 ["id1"],"full")
#                   .withColumn("id",expr("case when id is null then id1 else id end"))
#                   .orderBy("id")
#                   .drop("id1")
# )
# fulljoin.show()



### SCENARIO 1

# source_rdd = spark.sparkContext.parallelize([
#     (1, "A"),
#     (2, "B"),
#     (3, "C"),
#     (4, "D")
# ],1)
#
# target_rdd = spark.sparkContext.parallelize([
#     (1, "A"),
#     (2, "B"),
#     (4, "X"),
#     (5, "F")
# ],2)
#
# # Convert RDDs to DataFrames using toDF()
# df1 = source_rdd.toDF(["id", "name"])
# df2 = target_rdd.toDF(["id", "name1"])
#
# # Show the DataFrames
# df1.show()
# df2.show()
#
# print("===== FULL JOIN=====")
#
# fulljoin = df1.join( df2, ["id"], "full" )
# fulljoin.show()
#
# from pyspark.sql.functions import *
#
# print("=====NAME AND NAME 1 MATCH=====")
#
# fulljoin = df1.join( df2, ["id"], "full" )
# fulljoin.show()
#
# procdf = fulljoin.withColumn("status",expr("case when name=name1 then 'match' else 'mismatch' end"))
# procdf.show()
#
# print("=====FILTER MISMATCH=====")
#
# fildf = procdf.filter("status='mismatch'")
# fildf.show()
#
# print("=====NULL CHECKS=====")
#
# procdf1 = (
#
#     fildf.withColumn("status",expr("""
#                                             case
#                                             when name1 is null then 'New In Source'
#                                             when name is null  then 'New In target'
#                                             else
#                                             status
#                                             end
#
#
#                                             """))
#
# )
#
# procdf1.show()
#
#
# print("=====FINAL PROC=====")
#
#
# finaldf = procdf1.drop("name","name1").withColumnRenamed("status","comment")
#
# finaldf.show()


# ##SCENARIO
# data = [(1,"Veg Biryani"),(2,"Veg Fried Rice"),(3,"Kaju Fried Rice"),(4,"Chicken Biryani"),(5,"Chicken Dum Biryani"),(6,"Prawns Biryani"),(7,"Fish Birayani")]
#
# df1 = spark.createDataFrame(data,["food_id","food_item"])
# df1.show()
#
# ratings = [(1,5),(2,3),(3,4),(4,4),(5,5),(6,4),(7,4)]
#
# df2 = spark.createDataFrame(ratings,["food_id","rating"])
# df2.show()
#
#
# from pyspark.sql.functions import *
#
# leftjoin = df1.join(df2,["food_id"], "left").orderBy("food_id").withColumn("stats(out of 5)",expr("repeat('*',rating)"))
#
# leftjoin.show()


# ANTI JOIN AND CROSS JOIN



# data4 = [
#     (1, "raj"),
#     (2, "ravi"),
#     (3, "sai"),
#     (5, "rani")
# ]
#
# df1 = spark.createDataFrame(data4, ["id", "name"])
# df1.show()
#
# data3 = [
#     (1, "mouse"),
#     (3, "mobile"),
#     (7, "laptop")
# ]
#
# df2 = spark.createDataFrame(data3, ["id", "product"])
# df2.show()
#
#
#
#
# #convert dataframe into list
# listval = df2.select("id").rdd.flatMap(lambda x : x).collect()
# print(listval)
#
# from pyspark.sql import functions as F
#
# finaldf = df1.filter(~F.col('id').isin(listval))
# finaldf.show()

## left anti join- take left table and remove the ids from left table which are present in right table
# leftantijoin = df1.join(df2, ["id"],"left_anti").orderBy("id")
# print(" === Left Anti Join ==== ")
# leftantijoin.show()
#
# ## for right anti join just interchange the dataframe name only, no concept of "right_anti"
# rightantijoin = df2.join(df1, ["id"],"left_anti").orderBy("id")
# print(" === Right Anti Join ==== ")
# rightantijoin.show()
#
# #cross join is like one to many scenario
# crossjoin = df1.crossJoin(df2.withColumnRenamed("id","id1"))
# print(" === Cross Join ==== ")
# crossjoin.show()




##SCENARIO
#  CHILD,GRAND PARENTS,PARENTS SCENARIO

# data = [("A", "AA"), ("B", "BB"), ("C", "CC"), ("AA", "AAA"), ("BB", "BBB"), ("CC", "CCC")]
#
# df = spark.createDataFrame(data, ["child", "parent"])
# df.show()
#
#
# df1  = df
#
# df2  = df.withColumnRenamed("child","child1").withColumnRenamed("parent","parent1")
#
# df1.show()
# df2.show()
#
#
# joindf = df1.join(df2, df1["child"]==df2["parent1"] ,"inner")
# joindf.show()
#
# finaldf = (joindf.drop("parent1"))
# print("finaldf")
# finaldf.show()
#
# dffinal = (
#                 finaldf.withColumnRenamed("parent","parent1")
#                        .withColumnRenamed("child","parent")
#                        .withColumnRenamed("child1","child")
#                        .withColumnRenamed("parent1","grandparent")
#                        .select("child","parent","grandparent")
# )
#
# dffinal.show()


##AGGREGATION

# data = [("sai", 40), ("zeyo", 30), ("sai", 50), ("zeyo", 40), ("sai", 10)]
#
# df = spark.createDataFrame(data, ["name", "amount"])
#
# df.show()
# df.printSchema()
#
# from  pyspark.sql.functions import *
#
# aggdf = ( df.groupby("name")
#            .agg(sum("amount").alias("total"),
#                count("name").alias("cnt")
#
#
#
#         )
#           )
# aggdf.show()
#
#
# data1 = [("sai","chennai", 40), ("sai","hydb", 50), ("sai","chennai", 10), ("sai","hydb", 60)]
#
# df1 = spark.createDataFrame(data1, ["name", "location", "amount"])
#
# df1.show()
# df1.printSchema()
#
#
# from pyspark.sql.functions import *
#
# aggdf1 =(
#
#     df1.groupby("name","location")
#     .agg(sum("amount").alias("total"))
#
# )
#
# aggdf1.show()


##### scenario
# data1 = [
#     (1, "A", "A", 1000000),
#     (2, "B", "A", 2500000),
#     (3, "C", "G", 500000),
#     (4, "D", "G", 800000),
#     (5, "E", "W", 9000000),
#     (6, "F", "W", 2000000),
# ]
# df1 = spark.createDataFrame(data1, ["emp_id", "name", "dept_id", "salary"])
# df1.show()
#
# data2 = [("A", "AZURE"), ("G", "GCP"), ("W", "AWS")]
# df2 = spark.createDataFrame(data2, ["dept_id1", "dept_name"])
# df2.show()
#
# joindf = df1.join(df2,df1["dept_id"] == df2['dept_id1'],"left")
# joindf.show()
#
# selectdf = joindf.select("emp_id","name","dept_name","salary").orderBy("dept_name","salary")
# selectdf.show()
#
# from pyspark.sql.functions import *
#
# exprdf = (
#
#     selectdf    .groupby("dept_name")
#                  .agg(min("salary").alias("salary"))
#
# )
# print("Minimum salary table")
# exprdf.show()
#
# finaldf = exprdf.join(df1,["salary"],"inner").drop("dept_id")
# print("final output")
# finaldf.select("emp_id","name","dept_name","salary").show()



##PIVOT SCENARIO
#column values becomes column names

from pyspark.sql.functions import *

data1 = [
    (1, "id","1001"),
    (1, "name","adi"),
    (2, "id","1002"),
    (2, "name","vas")]
columns = ["pid", "keys", "values"]
rdd = spark.sparkContext.parallelize(data1)
df1 = rdd.toDF(columns)
df1.show()

pivotdf1 = df1.groupBy("pid").pivot("keys").agg(first("values"))
print("pivot table 1")
pivotdf1.show()



data = [
    (101, "Eng", 90),
    (101, "Sci", 80),
    (101, "Mat", 95),
    (102, "Eng", 75),
    (102, "Sci", 85),
    (102, "Mat", 90)
]
columns = ["Id", "Subject", "Marks"]
rdd = spark.sparkContext.parallelize(data)
df = rdd.toDF(columns)
df.show()


pivotdf = df.groupBy("Id").pivot("Subject").agg(first("Marks"))
print("pivot table 2")
pivotdf.show()



#🔴 Today's scenario


#👉 Data preparation code

data = [
    ("SEA", "SF", 300),
    ("CHI", "SEA", 2000),
    ("SF", "SEA", 300),
    ("SEA", "CHI", 2000),
    ("SEA", "LND", 500),
    ("LND", "SEA", 500),
    ("LND", "CHI", 1000),
    ("CHI", "NDL", 180)]
df = spark.createDataFrame(data, ["from", "to", "dist"])

df.show()

df1= df
from pyspark.sql.functions import *
df1.show()
df2= df1.withColumnRenamed("from","from1").withColumnRenamed("to","to1").withColumnRenamed("dist","dist1")
df2.show()

joindf = df1.join(df2,(df1["from"] == df2["to1"]) & (df1["to"] == df2["from1"]),"inner")
joindf.show()


groupdf = joindf.groupBy("from","to").agg(sum(col("dist") + col("dist1")).alias("roundtrip_dist"))
groupdf.show()


filterdf = groupdf.filter("from<to").orderBy("roundtrip_dist")
filterdf.show()




## WINDOWING ( RANK, DENSE_RANK, ROW_NUMBER)

from pyspark.sql.functions import  *


data = [("DEPT3", 500),
        ("DEPT3", 200),
        ("DEPT1", 1000),
        ("DEPT1", 700),
        ("DEPT1", 700),
        ("DEPT1", 500),
        ("DEPT2", 400),
        ("DEPT2", 200)]
columns = ["dept", "salary"]
df = spark.createDataFrame(data, columns)
df.show()


from pyspark.sql.window import Window
# STEP 1  CREATE WINDOW ON DEPT WITH DESC ORDER OF SALARY

deptwindow = Window.partitionBy("dept").orderBy(col("salary").desc())

#STEP 2  Applying window on DF with Dense Rank
denserankdf = df.withColumn("drank",dense_rank().over(deptwindow))
denserankdf.show()

#STEP 3  Filter rank '2' and drop drank
finaldf = denserankdf.filter("drank = 2").drop("drank")
print("DENSE RANK")
finaldf.show()





rankdf = df.withColumn("drank",rank().over(deptwindow))
rankdf.show()
finaldf1 = rankdf.filter("drank = 2").drop("drank")
print("RANK")
finaldf1.show()



rownumdf = df.withColumn("drank",row_number().over(deptwindow))
rownumdf.show()
finaldf2 = rownumdf.filter("drank = 2").drop("drank")
print("ROW NUMBER")
finaldf2.show()


















