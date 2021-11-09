from pyspark.sql.functions import col, to_date

opt_dict = {'sep': ';', 'encoding': 'ISO-8859-1', "escape": "\""}

def read_csv(spark, path, schema):
    return (
                spark.read
                .format("csv")
                .options(**opt_dict)
                .schema(schema)
                .load(path)
            )
def read_parquet(spark, path):
    return spark.read.format('parquet').load(path)

def write_parquet(df, path):
    df.write.format('parquet').mode('overwrite').save(path)