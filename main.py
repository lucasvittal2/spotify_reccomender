from pyspark.sql import SparkSession
from pyspark import  SparkContext


from Utils.ProjectPathsSetup import ProjectPathsSetup
from Envioronment.Parameters import *

ProjectPathsSetup().add_project_paths(PROJECT_PATH)

from PreProcessing.PreProcessingPerformer import PreProcessingPerformer
from Advising.MusicAdvisor import MusicAdvisor


from Utils.SparkFileReader import SparkFileReader


sc  = SparkContext.getOrCreate()
ProjectPathsSetup().add_scripts_to_spark(PROJECT_PATH, sc)



spark = SparkSession.builder.appName("Recomendador Spark").getOrCreate()

#deactivate logs



#readFile
fileReader = SparkFileReader(spark_session = spark)
data = fileReader.get_file(DATA_URL)


fileReader = SparkFileReader(spark_session = spark)
genres_data = fileReader.get_file(DATA_URL_GENRES)



#PreProcess Data


inputCols = genres_data.drop(*['id','name','artists_song','artists']).columns
genres_data_preprocessor = PreProcessingPerformer(stages = ['VECTOR_ASSEMBLER', 'STANDARD_SCALER', 'PCA_DIM_REDUCTION'], inputCols= inputCols, outputCol= 'preprocessed_features', to_dim= DIM_RED)
genres_preprocessed_data = genres_data_preprocessor.process_data(genres_data)


# Advising musics

musicAdvisor = MusicAdvisor(spark) 

music_name = 'Ed Sheeran - Happier'
recommended_musics = musicAdvisor.advise_musics(genres_preprocessed_data, music_name, 15)
print(f"You were looking for {music_name}")
print(recommended_musics.select(['id', 'artists_song','Dist']).show(truncate=False))