from  abc import ABC, abstractmethod

class PreProcessor(ABC):
    
    def __init__(self):
        
        pass

    @abstractmethod
    def process_data(self, data):
        pass
    