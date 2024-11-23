import os
import urllib.request
import ssl

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

print("=====STARTED======")
data = sc.textFile("dt.txt")
print("====RAW DATA=====")
print()
data.foreach(print)
print()

mapsplit = data.map(lambda x : x.split(","))
print("====mapsplit DATA=====")
print()
mapsplit.foreach(print)
print()

from collections import namedtuple

columns = namedtuple("columns",["id","tno","amt","category","product","mode"])

assigncol = mapsplit.map(lambda x : columns(x[0],x[1],x[2],x[3],x[4],x[5]))


prodfilter = assigncol.filter(lambda x : "Gymnastics" in x.product)

print("====prodfilter DATA=====")
print()
prodfilter.foreach(print)
print()

df=prodfilter.toDF()
df.show()

df.createOrReplaceTempView("trump")

spark.sql("select id,tno from trump;").show()







