import numpy as np 
import re
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark import SparkContext

def format_checker(df, expr, col):
    rate = df.filter(df[col].rlike(expr)).count()/df.count() 
    return rate

def formating(string):
    expr = "(per)\s((Annum)|(Day)|(Hour))"
    if re.match(expr, string)==None:
        if "Annual" or "annual" or "year" or "Year" in string:
            return "per Annum"
        elif "Day" or "day" in string:
            return "per Day"
        elif "Hour" or "hour" in string:
            return "per Hour"
        else:
            return "per unrecognized period"
    else:
        return string

def validate(evl):
    udf_func = udf(formating, returnType=StringType())
    evl = evl.withColumn('New Pay Basis', udf_func(evl["Pay Basis"]))
    # evl.dtypes
    col = "Pay Basis"
    expr="^(per)\s.*"
    rate = format_checker(evl, expr, col)
    print("Original correct rate is "+str(rate))

    col = "New Pay Basis"
    expr="^(per)\s.*"
    rate = format_checker(evl, expr, col)
    print("New correct rate is "+str(rate))

