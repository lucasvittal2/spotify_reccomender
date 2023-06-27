from scipy.spatial.distance import euclidean



class EuclideanCalculator():
    
    def __init__(self):
        pass
    
    def calculate_distance(self, v1, v2):
        return euclidean(v1, v2)
    
