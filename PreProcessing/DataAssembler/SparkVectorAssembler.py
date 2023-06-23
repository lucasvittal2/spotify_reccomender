from pyspark.ml.feature import VectorAssembler
import sys
sys.path.append('../')


from Abstractractions.PreProcessor import PreProcessor

class SparkVectorAssembler(PreProcessor):
    
    def __init__(self, inputCols, outputCol ):
        self.vectorAssembler = VectorAssembler(inputCols = inputCols, outputCol = outputCol)
        
    def get_vectorAssembler(self):
        return self.vectorAssembler

    def process_data(self, data):
        return self.vectorAssembler.transform(data)