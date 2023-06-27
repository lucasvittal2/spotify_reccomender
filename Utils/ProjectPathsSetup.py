import sys
import os
from pyspark import SparkContext
class ProjectPathsSetup():
    
    def __init__(self):
        
        pass
        
    def __get_all_directories(self, directory):
        directories = []
        for root, _, _ in os.walk(directory):
            directories.append(root)
        return directories

 
    def __get_python_scripts(self, directory):
        python_scripts = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith(".py"):
                    python_scripts.append(os.path.join(root, file))
        return python_scripts    
    
    def add_project_paths(self,directory):
        
        all_directories = self.__get_all_directories(directory)

        for dir in all_directories:
            sys.path.append(dir)
            
    def add_scripts_to_spark(self, directory, spark_context: SparkContext):
        py_scripts = self.__get_python_scripts(directory)
        for script in py_scripts:
            spark_context.addFile(script)
                
   