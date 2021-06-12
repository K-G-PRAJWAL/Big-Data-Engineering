from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, DoubleType


def read_schema(schema_arg):
    d_types = {
        "StringType()": StringType(),
        "IntegerType()": IntegerType(),
        "TimestampType()": TimestampType(),
        "DoubleType()": DoubleType()
    }
    cols = schema_arg.split(',')
    schema = StructType()
    for col in cols:
        temp = col.split(" ")
        schema.add(temp[0], d_types[temp[1]], True)
    return schema
