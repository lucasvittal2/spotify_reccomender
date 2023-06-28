import ast
import pandas as pd
from pyspark.sql import SparkSession
from pyspark import  SparkContext
from pyspark.sql.functions import create_map, lit, col

from Utils.ProjectPathsSetup import ProjectPathsSetup
from Envioronment.Parameters import *

ProjectPathsSetup().add_project_paths(PROJECT_PATH)

from PreProcessing.PreProcessingPerformer import PreProcessingPerformer
from Advising.MusicAdvisor import MusicAdvisor
from SpotifyAPI.SpotifyAPIHandler import SpotifyAPIHandler
from Visuals.MusicVisualizator import MusicVisualizator
from pyspark.ml.linalg import DenseVector

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


#readFile and make adjustments to be read by spark jobs

genres_preprocessed_data_pandas = pd.read_csv(DATA_PATH + 'preprocessed_data.csv', sep=';')
genres_preprocessed_data_pandas['preprocessed_features'] = genres_preprocessed_data_pandas['preprocessed_features'].apply(lambda x: DenseVector(ast.literal_eval(x)))
genres_preprocessed_data = spark.createDataFrame(genres_preprocessed_data_pandas)





print('Got files !\n')



# Advising musics
print("*"*150)
print('Getting Advising music...')
musicAdvisor = MusicAdvisor(spark) 

music_name = 'Ed Sheeran - Happier'
recommended_musics = musicAdvisor.advise_musics(genres_preprocessed_data, music_name, 15)
print(f"You were looking for {music_name}")
print(recommended_musics.select(['id', 'artists_song','Dist']).show(truncate=False))

print("Got recommended musics!\n")
print("*"*150)

spotifyApi = SpotifyAPIHandler()
url, name = spotifyApi.get_music_data('5Nm9ERjJZ5oyfXZTECKmRt')

# get names and urls
print("*"*150)
print('Geting Musics data from Spotify...')
ids = recommended_musics.select('id').rdd.flatMap(lambda x: x).collect()
music_data = [ (url, name) for url,name in [spotifyApi.get_music_data(id) for id in ids] ]

urls = [ data[0] for data in music_data]
names = [ data[1] for data in music_data]
print('Got Music data from spotify!\n')
print("*"*150)

#Show music data

print('Building Your Visuals...')
MusicVisualizator().visualize_songs(names, urls)
print("Here's Your Musics!")
print("*"*150)

print("Programm Finished Successfully !\n")
print('='*150)