from pyspark.ml.feature import PCA
import sys
sys.path.append('../')


from Abstractractions.PreProcessor import PreProcessor



class SparkPCADimensionalReductor(PreProcessor):
    
    
    def __init__(self, inputCol: str, outputCol: str, to_dim: int):
        self.PCAModel = PCA(k= to_dim, inputCol=inputCol, outputCol= outputCol)
    
    
    def  get_PCAModel(self):
        return self.PCAModel
    
    def process_data(self, data):
        model_pca = self.PCAModel.fit(data)
        reduced_data= model_pca.transform(data)
        return reduced_data