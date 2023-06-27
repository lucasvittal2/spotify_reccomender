import numpy as np
from pyspark.sql import DataFrame, SparkSession
from Clusterization.KmeansClusterizator import KmeansClusterizator
from MusicDistCalculation.MusicDistanceCalculation import MusicDistanceCalculator


class MusicAdvisor():
    
    def __init__(self, spark_session: SparkSession):
        
        self.spark_session = spark_session
        self.musicClusterizator =  KmeansClusterizator(num_clusters = 50)
        self.musicDistCalculator = MusicDistanceCalculator(spark_session= spark_session)


    def __do_clusterization(self, data: DataFrame) -> DataFrame:
        print('Performing Clusterization...')
        data_with_cluster = self.musicClusterizator.set_clusters(data,'preprocessed_features', 'cluster')
        print('Clusterization Done! \n')
        return data_with_cluster
    
    def __filter_musics_by_cluster(self, data: DataFrame, cluster: int):
        print('Filtering by cluster')
        same_cluster_musics = data.filter(data.cluster == cluster)
        print('Musics on the same cluster collected!')
        return  same_cluster_musics
    
    def __get_distances(self, data: DataFrame, from_music_vector) -> DataFrame:
        
        print('Getting Distances between musics...')
        data_with_distances = self.musicDistCalculator.calculate_music_distances(data, from_music_vector,'preprocessed_features')
        print(' Music distances Calculation Done!!')
        return data_with_distances
    
    def __get_nearests(self, data: DataFrame, num_nears: int) -> DataFrame:
        
        print(f'Getting the {num_nears} musics...')
        nearests_musics  = self.spark_session.createDataFrame( data.sort('Dist').filter("Dist!=0").take(num_nears))
        print(f'got the {num_nears} nearest musics !')
        return nearests_musics
    
    
    def advise_musics(self, data:DataFrame, music_name: str,  musics: int) -> DataFrame:
        
        data_with_clusters = self.__do_clusterization(data)
        
        cluster = data_with_clusters.filter(data.artists_song == music_name).select('cluster').collect()[0][0]
        from_music_vector = data_with_clusters.filter(data.artists_song == music_name).select('preprocessed_features').collect()[0][0]
        from_music_vector = np.array(from_music_vector)
        
        same_music_clusters = self.__filter_musics_by_cluster(data_with_clusters,cluster)
        data_with_distances = self.__get_distances(same_music_clusters, from_music_vector)
        music_recommendations = self.__get_nearests(data_with_distances, musics)
        print('*')
        return music_recommendations
    
    