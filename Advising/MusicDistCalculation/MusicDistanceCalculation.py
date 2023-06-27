from EuclideanCalculator import EuclideanCalculator
from pyspark.sql import DataFrame, functions as f, SparkSession
from pyspark.sql.types import FloatType

from typing import List


class  MusicDistanceCalculator():
    
    def __init__(self, spark_session: SparkSession):
        self.calculator = EuclideanCalculator()
        self.spark_session = spark_session
    
    def calculate_music_distances(self, data: DataFrame, from_music_vect: List[float], col: str):
        
        tmp_data = data.withColumn("from_vec", f.lit(from_music_vect))
        udf_calculate_distance = f.udf( self.calculator.calculate_distance, FloatType() )
        music_distances  = tmp_data.withColumn('Dist', udf_calculate_distance('from_vec', 'preprocessed_features') )
        return music_distances
       