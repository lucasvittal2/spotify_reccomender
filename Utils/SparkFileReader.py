from pyspark.sql import SparkSession
from pyspark import SparkFiles

class SparkFileReader():
    
    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session
        
    def get_file(self, data_url):
        
        spark = self.spark_session
        spark.sparkContext.addFile(data_url)
        data_path = SparkFiles.get('dados_musicas.csv')
        data = spark.read.csv(data_path, header=True, sep=';', inferSchema=True)
        return data
    
    
    