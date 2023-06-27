import sys
sys.path.append('../../')

from pyspark.ml.clustering import KMeans
from Envioronment.Parameters import *



class KmeansClusterizator():
    
    def __init__(self, num_clusters):
        self.num_clusters = num_clusters
        
        
    def __train_knn(self, data, inputCol, outputCol):
        kmeans = KMeans(k= self.num_clusters, featuresCol=inputCol, predictionCol=outputCol, seed=SEED)
        kmeans_model = kmeans.fit(data)
        return kmeans_model
    
    def set_clusters(self, data, inputCol, outputCol):
        kmeans_model = self.__train_knn(data,inputCol, outputCol)
        return kmeans_model.transform(data)
    