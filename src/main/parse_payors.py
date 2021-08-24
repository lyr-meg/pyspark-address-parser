# -*- coding: utf-8 -*-
import re
import datetime
from dateutil.relativedelta import relativedelta
import json
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, ArrayType, Row
from pyspark.sql.functions import *
from pyspark.sql.column import Column, _to_java_column, _to_seq
import sys
import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
from constants import *

def pc_op_pos_finder(x, y):
    if x is not None and y!='':
        for i, e in enumerate(x):
            if y in e:
                return i
    return None

def extract_all(regex, s=None):
    if s:
        all_matches = re.findall(r"{}".format(regex), s)
        return all_matches

pc_op_pos_finder_udf = udf(pc_op_pos_finder, IntegerType())
extract_all_udf = udf(extract_all, ArrayType(StringType()))

class parse_symcor_payors:

    def __init__(self, spark, sc, sdf):
        self.spark = spark
        self.sdf = sdf
        self.sc = sc
    
    def filter_for_pattern(self, delimiters_count=None, pc_pos=None):
        return self.sdf.withColumn('payor_address_op', regexp_replace('payor_address_op', r'·', ' '))\
        .withColumn('payor_address_op_org', col('payor_address_op'))\
        .withColumn('pc_op', regexp_extract(col('payor_address_op'), regex_pc, 1))\
        .withColumn('payor_address_op_splitted', split(col('payor_address_op'), r'[;]'))\
        .withColumn('pc_op_pos', pc_op_pos_finder_udf(col('payor_address_op_splitted'), col('pc_op')))\
        .filter(col('delimiters_count')==delimiters_count).filter(col('pc_op_pos')==pc_pos)

    def extract_address(self, col):
        _test_udf = self.sc._jvm.com.bns.amlthreat.commonudfs.AddressMatch.extractAddress()
        return Column(_test_udf.apply(_to_seq(self.sc, [col], _to_java_column)))

    def strict_extract_fax_and_phone_number(self, sdf):
        return sdf.withColumn('fax_number_op', extract_all_udf(lit(strict_regex_fax_extract), col('payor_address_op')))\
            .withColumn('payor_address_op', regexp_replace(col('payor_address_op'), strict_regex_fax_replace, '')).\
                withColumn('phone_number_op', extract_all_udf(lit(strict_regex_phone_extract), col('payor_address_op'))).\
                    withColumn('payor_address_op', regexp_replace(col('payor_address_op'), strict_regex_phone_replace, ''))

    def extract_fax_and_phone_number(self, sdf):
        return sdf.withColumn('fax_number_op', extract_all_udf(lit(regex_fax_extract), col('payor_address_op')))\
            .withColumn('payor_address_op', regexp_replace(col('payor_address_op'), regex_fax_replace, '')).\
                withColumn('phone_number_op', extract_all_udf(lit(regex_phone_extract), col('payor_address_op'))).\
                    withColumn('payor_address_op', regexp_replace(col('payor_address_op'), regex_phone_replace, ''))

    def strict_extract_email_address(self, sdf):
        return sdf.withColumn('email_op', extract_all_udf(lit(regex_email_extract), col('payor_address_op')))\
            .withColumn('payor_address_op', regexp_replace(col('payor_address_op'), regex_email_replace, ''))

    def strict_extract_url(self, sdf):
        return sdf.withColumn('url_op', extract_all_udf(lit(regex_url_extract), col('payor_address_op')))\
            .withColumn('payor_address_op', regexp_replace(col('payor_address_op'), regex_url_replace, ''))

    def clean_city_prov_pc_op(self, sdf):
        return sdf.withColumn('city_prov_pc_op', regexp_replace(col('city_prov_pc_op'), regex_pc, ''))\
            .withColumn('city_prov_pc_op', regexp_replace(col('city_prov_pc_op'), r' +', ' '))\
                .withColumn('city_prov_pc_op', regexp_replace(col('city_prov_pc_op'), r'\(|\)|\bCA\b|\bCANADA\b', ''))

    def extract_prov_city(self, sdf, regex_prov_ca_extract):
        return sdf.withColumn('prov_op', regexp_extract(col('city_prov_pc_op'), r"({})".format(regex_prov_ca_extract), 1))\
            .withColumn('city_prov_pc_op', regexp_replace(col('city_prov_pc_op'), r"({})".format(regex_prov_ca_extract), ''))\
                .withColumn('city_op', regexp_replace(col('city_prov_pc_op'), r"(?i)[,]|\s+\bTEL\b[:,\s]{0,}|\s+\bAND\b", ''))

    def add_cols_delimiter_count_and_pc_pos(self, sdf, delimiter_count, pc_pos):
        return sdf.withColumn('delimiter_count', lit(delimiter_count))\
            .withColumn('pc_pos', lit(pc_pos))
    
    def separate_subpattern_by_digits_before_pc(self, sdf):
        split_by_delimiter = split(col('payor_address_op'), r'[;]')
        split_by_pc = split(col('payor_op_3rd_ele'), regex_pc)
        return sdf.withColumn('payor_op_3rd_ele', split_by_delimiter.getItem(2))\
        .withColumn('city_prov_pc_op', split_by_pc.getItem(0))\
        .withColumn('payor_op_3rd_ele_after_pc', split_by_pc.getItem(1))\
        .withColumn('pattern1_ind', regexp_extract(col('city_prov_pc_op'), r"(\d+)", 1))
    
    def get_subpattern_dfs(self, sdf):
        return self.separate_subpattern_by_digits_before_pc(sdf).filter(col('pattern1_ind')==""), \
        self.separate_subpattern_by_digits_before_pc(sdf).filter(col('pattern1_ind')!="")
    
    def separate_subpattern_by_4th_ele(self, sdf):
        split_by_delimiter = split(col('payor_address_op'), r'[;]')
        return sdf.withColumn('payor_op_4th_ele', split_by_delimiter.getItem(3))\
        .withColumn('pattern1_ind', regexp_replace(col('payor_op_4th_ele'), regex_pc, ''))\
        .withColumn('pattern1_ind', trim(col("pattern1_ind")))

    def extract_entity_name1_and_street_address(self, sdf, entity_name1_pos=0, street_address_pos=1):
        split_by_delimiter = split(col('payor_address_op'), r'[;]')
        return sdf.withColumn('entity_name1_op', split_by_delimiter.getItem(entity_name1_pos))\
        .withColumn('street_address_op', split_by_delimiter.getItem(street_address_pos))
    
    def extract_phone_fax_number(self, sdf, phone_fax_number_pos=3):
        split_by_delimiter = split(col('payor_address_op'), r'[;]')
        return sdf.withColumn('phone_fax_op', split_by_delimiter.getItem(phone_fax_number_pos))
    
    def extract_entity_names(self, sdf, entity_name1_pos=0, entity_name2_pos=1):
        split_by_delimiter = split(col('payor_address_op'), r'[;]')
        return sdf.withColumn('entity_name1_op', split_by_delimiter.getItem(entity_name1_pos))\
        .withColumn('entity_name2_op', split_by_delimiter.getItem(entity_name2_pos))
    
    def create_address_construct(self, sdf):
        return sdf.na.fill("").withColumn(\
            "addressStruct", struct(col("street_address_op").alias("address"), col("pc_op").alias("postalcode"),col("city_op").alias("city"),col("prov_op").alias("province"),col("country_op").alias("country"))\
        ).withColumn("extracted", self.extract_address(col("addressStruct")))
    
    def explode_mapping_col(self, sdf):
        distinctKeys = sdf.limit(1).select(explode("extracted")).agg(collect_set("key").alias('keys')).first().keys
        return sdf.select("isn", "business_date", "flag", "payor_address_op_org", "entity_name1_op", "entity_name2_op",\
            "phone_number_op", "fax_number_op", "email_op", "url_op", \
            "street_address_op", "pc_op", "city_op", "prov_op", "country_op", \
            *[ col("extracted")[k].alias(k) for k in distinctKeys ])
    
    def prepare_input_libpostal(self, sdf):
        return sdf.withColumnRenamed("city_prov_pc_op", "street_city_prov_pc_op")\
            .withColumn("street_city_prov_pc_op", concat(col("street_city_prov_pc_op"), lit(" CA")))
    
    def organize_output_libpostal(self, sdf):
        return sdf.withColumn("street_address_op", concat(col("op_lp_house"), lit(" "), col("op_lp_unit"), lit(" "), col("op_lp_house_number"), lit(" "), col("op_lp_road"), lit(" "), col("op_lp_po_box")))\
        .withColumnRenamed("op_lp_city", "city_op").withColumnRenamed("op_lp_province", "prov_op").withColumnRenamed("op_lp_country", "country_op")
    
    def save_table(self, sdf, table_name):
        sdf.write.option("encoding", "ISO-8859-1").mode("overwrite").format("orc").saveAsTable(table_name)
    
    def parse_d2p2(self):
        pyLibPostal = self.sc._jvm.com.bns.amlthreat.commonudfs.PyLibPostal
        lipPostal = pyLibPostal.init(self.sc._jsc)
        sqlContext = SQLContext(self.sc)
        # extract postal code and find position of postal code in payor string and filter out pattern
        df_d2p2 = self.filter_for_pattern(delimiters_count=2, pc_pos=2)
        df_d2p2 = self.strict_extract_fax_and_phone_number(df_d2p2)
        df_d2p2 = self.strict_extract_email_address(df_d2p2)
        df_d2p2 = self.strict_extract_url(df_d2p2)
        ############### Step 2: Separate Pattern 1 and Pattern 2 Payors
        df_d2p2_p1, df_d2p2_p2 = self.get_subpattern_dfs(df_d2p2)
        ############### Step 3: Parse payors following pattern 1
        # Get the 1st element as entity name and the 2nd element as street address
        df_d2p2_p1 = self.extract_entity_name1_and_street_address(df_d2p2_p1, entity_name1_pos=0, street_address_pos=1)
        # Get the 3rd element, extract any string before postal code, remove extra blanks and remove other noisy parts
        df_d2p2_p1 = self.clean_city_prov_pc_op(df_d2p2_p1)
        # Clean up and standardize province names to be abbreviations in the extracted 3rd string element leveraging a pre-constructed province spelling dictionary
        regex_prov_ca_extract = ""
        for key in dict_prov_ca:
            df_d2p2_p1 = df_d2p2_p1.withColumn('city_prov_pc_op', regexp_replace(col('city_prov_pc_op'), r"\b{}(?=\s|,|$)".format(key), dict_prov_ca[key]))
            regex_prov_ca_extract = regex_prov_ca_extract + "\\b" + r"{}".format(dict_prov_ca[key]) + "\\b|"
        # Extract province abbreviations in the 3rd element and remove province abbreviations from the 3rd element
        # Clean up and extract city names from the 3rd element
        regex_prov_ca_extract = regex_prov_ca_extract[:-1]
        df_d2p2_p1 = self.extract_prov_city(df_d2p2_p1, regex_prov_ca_extract)
        # Create an address construct variable containing address, postal code, city, province and country
        # Leverage Extract_Address utitlity in commonudfs to extract address components
        df_d2p2_p1 = df_d2p2_p1.withColumn('country_op', lit('CA'))
        df_d2p2_p1 = self.create_address_construct(df_d2p2_p1)
        # Explode returned mapping address column to be address columns
        df_d2p2_p1 = df_d2p2_p1.withColumn('entity_name2_op', lit(""))
        df_d2p2_p1_final = self.explode_mapping_col(df_d2p2_p1)
        ############### Step 4: Parse payors following pattern 2
        # Get the 1st element as entity name 1 and the 2nd element as entity name 2
        df_d2p2_p2 = self.extract_entity_names(df_d2p2_p2, entity_name1_pos=0, entity_name2_pos=1)
        # Get the 3rd element, parse using libpostal to get back house, unit, house number, road, postal code, city, province, country and po box
        df_d2p2_p2 = self.prepare_input_libpostal(df_d2p2_p2)
        lp_results = lipPostal.parse(df_d2p2_p2.na.fill("")._jdf, "street_city_prov_pc_op", "op_lp_")
        lp_results.createOrReplaceTempView("temp_table")
        df_d2p2_p2_lp = sqlContext.table("temp_table")
        # Concat house, unit, house number, road, po box as address and create an address construct variable containing address, postal code, city, province and country
        df_d2p2_p2_lp = self.organize_output_libpostal(df_d2p2_p2_lp)
        # Leverage Extract_Address utitlity in commonudfs to extract address components
        df_d2p2_p2_lp = self.create_address_construct(df_d2p2_p2_lp)
        # Explode returned mapping address column to be address columns
        df_d2p2_p2_final = self.explode_mapping_col(df_d2p2_p2_lp)
        ############### Step 5: Concat Pattern 1 and Pattern 2 Results and Write to EDL
        df_d2p2_final = df_d2p2_p1_final.union(df_d2p2_p2_final)
        df_d2p2_final = self.add_cols_delimiter_count_and_pc_pos(df_d2p2_final, 2, 2)
        self.save_table(df_d2p2_final, d2p2_table)
    
    def parse_d3p2(self):
        pyLibPostal = self.sc._jvm.com.bns.amlthreat.commonudfs.PyLibPostal
        lipPostal = pyLibPostal.init(self.sc._jsc)
        sqlContext = SQLContext(self.sc)
        # extract postal code and find position of postal code in payor string and filter out pattern
        df_d3p2 = self.filter_for_pattern(delimiters_count=3, pc_pos=2)
        df_d3p2 = self.strict_extract_email_address(df_d3p2)
        df_d3p2 = self.strict_extract_url(df_d3p2)
        ############### Step 2: Separate Pattern 1 and Pattern 2 Payors
        df_d3p2_p1, df_d3p2_p2 = self.get_subpattern_dfs(df_d3p2)
        ############### Step 3: Parse payors following pattern 1
        # Get the 1st element as entity name and the 2nd element as street address, the 4th element as fax and phone number
        df_d3p2_p1 = self.extract_entity_name1_and_street_address(df_d3p2_p1, entity_name1_pos=0, street_address_pos=1)
        df_d3p2_p1 = self.extract_phone_fax_number(df_d3p2_p1, phone_fax_number_pos=3)
        # Get the 4th element, extract fax numbers if exist using regex and remove fax numbers from the 4th element, 
        # extract phone numbers from the 4th element if exist using regex
        df_d3p2_p1 = self.extract_fax_and_phone_number(df_d3p2_p1)
        # Get the 3rd element, extract any string before postal code, remove extra blanks and remove other noisy parts
        df_d3p2_p1 = self.clean_city_prov_pc_op(df_d3p2_p1)
        # Clean up and standardize province names to be abbreviations in the extracted 3rd string element leveraging a pre-constructed province spelling dictionary
        regex_prov_ca_extract = ""
        for key in dict_prov_ca:
            df_d3p2_p1 = df_d3p2_p1.withColumn('city_prov_pc_op', regexp_replace(col('city_prov_pc_op'), r"\b{}(?=\s|,|$)".format(key), dict_prov_ca[key]))
            regex_prov_ca_extract = regex_prov_ca_extract + "\\b" + r"{}".format(dict_prov_ca[key]) + "\\b|"
        # Extract province abbreviations in the 3rd element and remove province abbreviations from the 3rd element
        # Clean up and extract city names from the 3rd element
        regex_prov_ca_extract = regex_prov_ca_extract[:-1]
        df_d3p2_p1 = self.extract_prov_city(df_d3p2_p1, regex_prov_ca_extract)
        # Create an address construct variable containing address, postal code, city, province and country
        # Leverage Extract_Address utitlity in commonudfs to extract address components
        df_d3p2_p1 = df_d3p2_p1.withColumn('country_op', lit('CA'))
        df_d3p2_p1 = self.create_address_construct(df_d3p2_p1)
        # Explode returned mapping address column to be address columns
        df_d3p2_p1 = df_d3p2_p1.withColumn('entity_name2_op', lit(""))
        df_d3p2_p1_final = self.explode_mapping_col(df_d3p2_p1)
        # Get the 1st element as entity name 1, the 2nd element as entity name 2 and the 4th element as fax and phone number
        df_d3p2_p2 = self.extract_entity_names(df_d3p2_p2, entity_name1_pos=0, entity_name2_pos=1)
        df_d3p2_p2 = self.extract_phone_fax_number(df_d3p2_p2, phone_fax_number_pos=3)
        # Extract fax numbers if exist using regex and remove fax numbers from the 4th element 
        # Extract phone numbers from the 4th element if exist using regex
        df_d3p2_p2 = self.extract_fax_and_phone_number(df_d3p2_p2)
        # Get the 3rd element, parse using libpostal to get back house, unit, house number, road, postal code, city, province, country and po box
        df_d3p2_p2 = self.prepare_input_libpostal(df_d3p2_p2)
        lp_results = lipPostal.parse(df_d3p2_p2.na.fill("")._jdf, "street_city_prov_pc_op", "op_lp_")
        lp_results.createOrReplaceTempView("temp_table")
        df_d3p2_p2_lp = sqlContext.table("temp_table")
        # Concat house, unit, house number, road, po box as address and create an address construct variable containing address, postal code, city, province and country
        df_d3p2_p2_lp = self.organize_output_libpostal(df_d3p2_p2_lp)
        # Leverage Extract_Address utitlity in commonudfs to extract address components
        df_d3p2_p2_lp = self.create_address_construct(df_d3p2_p2_lp)
        # Explode returned mapping address column to be address columns
        df_d3p2_p2_final = self.explode_mapping_col(df_d3p2_p2_lp)
        ############### Step 5: Concat Pattern 1 and Pattern 2 Results and Write to EDL
        df_d3p2_final = df_d3p2_p1_final.union(df_d3p2_p2_final)
        df_d3p2_final = self.add_cols_delimiter_count_and_pc_pos(df_d3p2_final, 3, 2)
        self.save_table(df_d3p2_final, d3p2_table)
    
    def parse_d4p3(self):
        pyLibPostal = self.sc._jvm.com.bns.amlthreat.commonudfs.PyLibPostal
        lipPostal = pyLibPostal.init(self.sc._jsc)
        sqlContext = SQLContext(self.sc)
        # extract postal code and find position of postal code in payor string and filter out pattern
        df_d4p3 = self.filter_for_pattern(delimiters_count=4, pc_pos=3)
        df_d4p3 = self.strict_extract_email_address(df_d4p3)
        df_d4p3 = self.strict_extract_url(df_d4p3)
        ############### Step 2: Separate Pattern 1 and Pattern 2 Payors
        df_d4p3 = self.separate_subpattern_by_4th_ele(df_d4p3)        
        df_d4p3_p1 = df_d4p3.filter(df_d4p3.pattern1_ind!="")
        df_d4p3_p2 = df_d4p3.filter(df_d4p3.pattern1_ind=="")
        ############### Step 3: Parse payors following pattern 1
        # Get the 1st element as entity name 1, the 2nd element as entity name 2, the 3rd element as street address and the 5th element as fax and phone number
        df_d4p3_p1 = self.extract_entity_names(df_d4p3_p1, entity_name1_pos=0, entity_name2_pos=1)
        df_d4p3_p1 = self.extract_phone_fax_number(df_d4p3_p1, phone_fax_number_pos=4)
        split_by_delimiter = split(col('payor_address_op'), r'[;]')
        df_d4p3_p1 = df_d4p3_p1.withColumn('street_address_op', split_by_delimiter.getItem(2))
        df_d4p3_p1 = df_d4p3_p1.withColumnRenamed("payor_op_4th_ele", "city_prov_pc_op")
        # Get the 5th element, extract fax numbers if exist using regex and remove fax numbers from the 5th element, 
        # extract phone numbers from the 5th element if exist using regex
        df_d4p3_p1 = self.extract_fax_and_phone_number(df_d4p3_p1)
        # Get the 4th element, remove the extracted postal code, any extra blanks and remove other noisy parts
        df_d4p3_p1 = self.clean_city_prov_pc_op(df_d4p3_p1)
        # Clean up and standardize province names to be abbreviations in the 4th element leveraging a pre-constructed province spelling dictionary
        regex_prov_ca_extract = ""
        for key in dict_prov_ca:
            df_d4p3_p1 = df_d4p3_p1.withColumn('city_prov_pc_op', regexp_replace(col('city_prov_pc_op'), r"\b{}(?=\s|,|$)".format(key), dict_prov_ca[key]))
            regex_prov_ca_extract = regex_prov_ca_extract + "\\b" + r"{}".format(dict_prov_ca[key]) + "\\b|"
        # Extract province abbreviations in the 4th element and remove province abbreviations from the 4th element
        # Clean up and extract city names from the 4th element
        regex_prov_ca_extract = regex_prov_ca_extract[:-1]
        df_d4p3_p1 = self.extract_prov_city(df_d4p3_p1, regex_prov_ca_extract)
        # Create an address construct variable containing address, postal code, city, province and country
        # Leverage Extract_Address utitlity in commonudfs to extract address components
        df_d4p3_p1 = df_d4p3_p1.withColumn('country_op', lit('CA'))
        df_d4p3_p1 = self.create_address_construct(df_d4p3_p1)
        # Explode returned mapping address column to be address columns
        df_d4p3_p1_final = self.explode_mapping_col(df_d4p3_p1)
        ############### Step 4: Parse payors following pattern 2
        # Get the 1st element as entity name 1, the 2nd element as street address and the 5th element as fax and phone number
        df_d4p3_p2 = self.extract_entity_name1_and_street_address(df_d4p3_p2, entity_name1_pos=0, street_address_pos=1)
        df_d4p3_p2 = self.extract_phone_fax_number(df_d4p3_p2, phone_fax_number_pos=4)
        df_d4p3_p2 = df_d4p3_p2.withColumn('city_prov_pc_op', split_by_delimiter.getItem(2))
        # Extract fax numbers if exist using regex and remove fax numbers from the 5th element, 
        # extract phone numbers from the 5th element if exist using regex
        df_d4p3_p2 = self.extract_fax_and_phone_number(df_d4p3_p2)
        # Get the 3rd element, remove the extracted postal code, any extra blanks and remove other noisy parts
        df_d4p3_p2 = self.clean_city_prov_pc_op(df_d4p3_p2)
        # Clean up and standardize province names to be abbreviations in the 3rd element leveraging a pre-constructed province spelling dictionary
        regex_prov_ca_extract = ""
        for key in dict_prov_ca:
            df_d4p3_p2 = df_d4p3_p2.withColumn('city_prov_pc_op', regexp_replace(col('city_prov_pc_op'), r"\b{}(?=\s|,|$)".format(key), dict_prov_ca[key]))
            regex_prov_ca_extract = regex_prov_ca_extract + "\\b" + r"{}".format(dict_prov_ca[key]) + "\\b|"
        # Extract province abbreviations in the 3rd element and remove province abbreviations from the 4th element
        # Clean up and extract city names from the 3rd element
        regex_prov_ca_extract = regex_prov_ca_extract[:-1]
        df_d4p3_p2 = self.extract_prov_city(df_d4p3_p2, regex_prov_ca_extract)
        # Create an address construct variable containing address, postal code, city, province and country
        # Leverage Extract_Address utitlity in commonudfs to extract address components
        df_d4p3_p2 = df_d4p3_p2.withColumn('country_op', lit('CA'))
        df_d4p3_p2 = self.create_address_construct(df_d4p3_p2)
        # Explode returned mapping address column to be address columns
        df_d4p3_p2 = df_d4p3_p2.withColumn('entity_name2_op', lit(""))
        df_d4p3_p2_final = self.explode_mapping_col(df_d4p3_p2)
        ############### Step 5: Concat Pattern 1 and Pattern 2 Results and Write to EDL
        df_d4p3_final = df_d4p3_p1_final.union(df_d4p3_p2_final)
        df_d4p3_final = self.add_cols_delimiter_count_and_pc_pos(df_d4p3_final, 4, 3)
        self.save_table(df_d4p3_final, d4p3_table)

































