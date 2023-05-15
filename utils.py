from pyspark.sql.functions import desc
# File Read

def read_dataframe(spark, file : str):
    return  spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(file)

def filter_data(data):
    return data.where(data['Business Account Number'] < 750) \
            .select('Business Account Number', 'City', 'Mail Zipcode') \
            .orderBy(desc('Business Account Number'))
