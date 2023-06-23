import sys
sys.path.append('../')


from Abstractractions.PreProcessor import PreProcessor
from pyspark.ml.feature import MinMaxScaler

class MinMaxScaler(PreProcessor):
    
    def __init__(self, inputCol: str, outputCol: str ):
        self.minMaxScaler = MinMaxScaler(inputCol= inputCol, outputCol = outputCol)
        

    def get_scaler(self):
        return self.minMaxScaler
    
    def process_data(self, data):
        scaler_model = self.minMaxScaler.fit(data)
        scaled_data = scaler_model.transform(data)
        return  scaled_data