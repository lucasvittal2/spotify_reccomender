from pyspark.sql import SparkSession
from pyspark import SparkFiles
import warnings
import sys
sys.path.append('PreProcessing')

from PreProcessing.PreProcessingPerformer import PreProcessingPerformer
warnings.filterwarnings("ignore")

from Envioronment.Parameters import *
from Utils.SparkFileReader import SparkFileReader
spark = SparkSession.builder.appName("Recomendador Spark").getOrCreate()

#deactivate logs
spark.sparkContext.setLogLevel("ERROR")


#readFile
fileReader = SparkFileReader(spark_session = spark)
data = fileReader.get_file(DATA_URL)


fileReader = SparkFileReader(spark_session = spark)
genres_data = fileReader.get_file(DATA_URL_GENRES)



#PreProcess Data


inputCols = genres_data.drop(*['id','name','artists_song','artists']).columns
genres_data_preprocessor = PreProcessingPerformer(stages = ['VECTOR_ASSEMBLER', 'STANDARD_SCALER', 'PCA_DIM_REDUCTION'], inputCols= inputCols, outputCol= 'preprocessed_features', to_dim= DIM_RED)
genres_preprocessed_data = genres_data_preprocessor.process_data(genres_data)

print(genres_preprocessed_data.select('preprocessed_features').show())