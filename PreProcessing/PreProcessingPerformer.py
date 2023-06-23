from pyspark.ml import Pipeline
from typing import List

from Abstractractions.PreProcessor import PreProcessor
from DataAssembler.SparkVectorAssembler import SparkVectorAssembler
from DimReduction.SparkPCADimensionalReductor import SparkPCADimensionalReductor
from Scaling.SparkStandardScaler import SparkStandardScaler


class PreProcessingPerformer(PreProcessor):
    
    def __init__(self, stages: List[str], inputCols, outputCol, to_dim = 2):
        self.stages = stages
        self.inputCols= inputCols
        self.outputCol = outputCol
        self.to_dim = to_dim
    
    def __stage_translator(self):
        
        stages = self.stages
        preprocessors = []
        for stage in stages:
            if stage == 'VECTOR_ASSEMBLER':
                vectorAssembler = SparkVectorAssembler(inputCols= self.inputCols, outputCol='features').get_vectorAssembler()
                preprocessors.append(vectorAssembler)
                
            elif stage == 'STANDARD_SCALER':
                stdScaler = SparkStandardScaler(inputCol='features', outputCol = 'scaled_features').get_scaler()
                preprocessors.append(stdScaler)
                
            elif stage == 'PCA_DIM_REDUCTION':
                dim = self.to_dim
                pcaReduction = SparkPCADimensionalReductor(inputCol='scaled_features', outputCol= self.outputCol,to_dim=dim  ).get_PCAModel()
                preprocessors.append(pcaReduction)
        
        return preprocessors
                
    
    def process_data(self, data):
        preprocessing_stages = self.__stage_translator()
        pipeline = Pipeline(stages = preprocessing_stages)
        pipeline_model = pipeline.fit(data)
        return pipeline_model.transform(data)
    