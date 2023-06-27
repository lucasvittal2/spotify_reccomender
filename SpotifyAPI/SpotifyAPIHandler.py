import spotipy

from Envioronment.SpotAuthParams  import *
from SpotifyAPI.SpotifyAuthenticator import SpotifyAuthenticator
class SpotifyAPIHandler():
    
    def __init__(self ):
        self.spotAuthenticator = SpotifyAuthenticator(SCOPE, CLIENT_ID, CLIENT_SECRET, REDIRECT_URI)
    
    
    def __get_AuthApi(self):
        return self.spotAuthenticator.authenticate_spotify_api()
    

    def get_music_data(self, id: str):
        
        #auth
        spotAPI = self.__get_AuthApi()
        
        #get all data
        track = spotAPI.track(id)
        
        #get necessary data
        track_url = track["album"]["images"][1]["url"]
        track_name = track["name"]
        
        return track_url, track_name
   