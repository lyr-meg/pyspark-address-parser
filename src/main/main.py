from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext, HiveContext
import sys
import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
from parse_payors import *

if __name__ == "__main__":

    warehouse_location = abspath('spark-warehouse')
    spark = SparkSession \
        .builder    \
        .appName('Symcor Parse')  \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .config("spark.hive.mapred.supports.subdirectories", True) \
        .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", True) \
        .config("hive.exec.dynamic.partition", True) \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    hc = HiveContext(sc)
    sc.addFile("src/main/parse_payors.py")
    sc.addFile("src/main/constants.py")

    sdf = hc.sql("""select *, length(payor_address_op) - length(regexp_replace(payor_address_op,'\073','')) as delimiters_count 
    from caz_amltm_secure.ml_mds_at_prop_symcor_ocr_results_0623""")

    parse = parse_symcor_payors(spark, sc, sdf)

    parse.parse_d2p2()
    print("D2P2 TABLE IS READY!")

    parse.parse_d3p2()
    print("D3P2 TABLE IS READY!")

    parse.parse_d4p3()
    print("D4P3 TABLE IS READY!")





