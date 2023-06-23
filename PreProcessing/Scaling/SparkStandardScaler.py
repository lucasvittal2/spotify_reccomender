import sys
sys.path.append('../')


from Abstractractions.PreProcessor import PreProcessor
from pyspark.ml.feature import StandardScaler



class SparkStandardScaler(PreProcessor):
    
    def __init__(self, inputCol: str, outputCol: str ):
        self.stdScaler = StandardScaler(inputCol= inputCol, outputCol = outputCol)
        

    def get_scaler(self):
        return self.stdScaler
    
    def process_data(self, data):
        scaler_model = self.stdScaler.fit(data)
        scaled_data = scaler_model.transform(data)
        return  scaled_data