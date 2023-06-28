
from pyspark.sql import SparkSession
from pyspark import  SparkContext
import sys
sys.path.append('../')

from Utils.ProjectPathsSetup import ProjectPathsSetup
from Envioronment.Parameters import *

ProjectPathsSetup().add_project_paths(PROJECT_PATH)

from PreProcessing.PreProcessingPerformer import PreProcessingPerformer
from Utils.SparkFileReader import SparkFileReader

print('='*150)
print("*"*150)
print('Initializing Spark and configure setup...')
sc  = SparkContext.getOrCreate()
ProjectPathsSetup().add_scripts_to_spark(PROJECT_PATH, sc)



spark = SparkSession.builder.appName("Recomendador Spark").getOrCreate()

print('System ready to do its word !\n')
print("*"*150)

#deactivate logs


print('reading files...')


#readFile
fileReader = SparkFileReader(spark_session = spark)
data = fileReader.get_file(DATA_URL)


fileReader = SparkFileReader(spark_session = spark)
genres_data = fileReader.get_file(DATA_URL_GENRES)
print('Got files !\n')


#PreProcess Data
print("*"*150)

print('PreProcessing data...')
inputCols = genres_data.drop(*['id','name','artists_song','artists']).columns
genres_data_preprocessor = PreProcessingPerformer(stages = ['VECTOR_ASSEMBLER', 'STANDARD_SCALER', 'PCA_DIM_REDUCTION'], inputCols= inputCols, outputCol= 'preprocessed_features', to_dim= DIM_RED)
genres_preprocessed_data = genres_data_preprocessor.process_data(genres_data)
print('Data PreProcessed\n')

print("Saving Data...")
genres_preprocessed_data.toPandas().to_csv(DATA_PATH + 'preprocessed_data.csv', sep=';')
print('Preprocessed Data saved!\n')

print('Preprocessing completed successfully !')