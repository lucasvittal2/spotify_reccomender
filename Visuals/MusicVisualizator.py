import matplotlib.pyplot as plt
from skimage import io


class MusicVisualizator():
    
    
    def __init__(self):
        pass
    
    def visualize_songs(self, names,url):
    
        plt.figure(figsize=(15,10))
        columns = 5
        
        for i, u in enumerate(url):
            ax = plt.subplot(len(url) // columns + 1, columns, i + 1)
            image = io.imread(u)
            plt.imshow(image)
            ax.get_yaxis().set_visible(False)
            plt.xticks(color = 'w', fontsize = 0.1)
            plt.yticks(color = 'w', fontsize = 0.1)
            plt.xlabel(names[i], fontsize = 10)
            plt.tight_layout(h_pad=0.7, w_pad=0)
            plt.subplots_adjust(wspace=None, hspace=None)
            plt.grid(visible=None)
            plt.grid(False)
            
        plt.show()