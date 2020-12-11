# This Python 3 environment comes with many helpful analytics libraries installed
# It is defined by the kaggle/python Docker image: https://github.com/kaggle/docker-python
# For example, here's several helpful packages to load

import numpy as np # linear algebra
from spellchecker import SpellChecker
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.feature import StopWordsRemover, RegexTokenizer
from csv import reader
import re
spark = SparkSession.builder.master("local[*]").getOrCreate()
# Input data files are available in the read-only "../input/" directory
# For example, running this (by clicking run or pressing Shift+Enter) will list all files under the input directory

import os
for dirname, _, filenames in os.walk('/kaggle/input'):
    for filename in filenames:
        print(os.path.join(dirname, filename))

# You can write up to 20GB to the current directory (/kaggle/working/) that gets preserved as output when you create a version using "Save & Run All" 
# You can also write temporary files to /kaggle/temp/, but they won't be saved outside of the current session
df = spark.read.csv('../input/agencyrevenue/Prel.csv', inferSchema = True, header = True)
#tem = spark.read.csv('../input/temtem/tem.csv', inferSchema = True, header = True)
tem = spark.read.csv('../input/temtem/tem.csv', inferSchema = True, header = True)
#wordlist = spark.read.csv('../input/dictionary/Book1.csv')
evl = spark.read.csv('../input/evaluete/Citywide_Payroll_Data__Fiscal_Year_sample.csv', inferSchema = True, header = True)

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

