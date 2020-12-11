import numpy as np
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *


def dropNullValueColumns(tem, percent_standard):
    tem_columns = tem.columns
    tem_null_dict = {col:tem.filter(tem[col].isNull()).count()/tem.count() for col in tem.columns}
    tem_df = tem
    for index, col in enumerate(tem_null_dict):
        if tem_null_dict[col] > percent_standard:
            tem_df = tem_df.drop(tem_columns[index])
            print('Column ' + col + ' is dropped from the dataframe.')
    return tem_df

def replace_numeric_nulls(input1, numeric_cols):
    for col in numeric_cols:
        mean_val = input1.select(mean(input1[col]).cast('int')).collect()
        m = mean_val[0][0]
        input1 = input1.na.fill(m,subset=[col])
    return input1

def replace_non_numeric_nulls(input2, non_numeric_cols):
    for col in non_numeric_cols:
        most_frequent = input2.groupby(col).count().orderBy("count", ascending=False).first()[0]
        input2 = input2.na.fill(most_frequent,subset=[col])
    return input2

def classify(df):
    non_numeric_cols = [item[0] for item in df.dtypes if item[1] == 'string']
    numeric_cols = [item[0] for item in df.dtypes if item[1] != 'string']
    return non_numeric_cols, numeric_cols

def null_values(df):
    df = dropNullValueColumns(df, 0.1)
    non_numeric_cols, numeric_cols = classify(df)
    df1 = replace_numeric_nulls(df, numeric_cols)
    df2 = replace_non_numeric_nulls(df, non_numeric_cols)
    null_dict_2 = {col:df2.filter(df2[col].isNull()).count()/df2.count() for col in df2.columns}
    print(null_dict_2)
    return df2