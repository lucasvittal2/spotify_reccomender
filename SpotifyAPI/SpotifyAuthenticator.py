from  spotipy.oauth2 import  SpotifyOAuth, SpotifyClientCredentials
import spotipy


class SpotifyAuthenticator():
    
    def __init__(self, scope, client_id, client_secret, redirect_url):
        
        self.scope=  scope
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_url= redirect_url
        
    def authenticate_spotify_api(self):
        
        OAuth = SpotifyOAuth(scope = self.scope, redirect_uri= self.redirect_url, client_id = self.client_id, client_secret = self.client_secret)
        client_credentials_manager = SpotifyClientCredentials(client_id = self.client_id, client_secret = self.client_secret)
        authenticated_spotify_api= spotipy.Spotify(client_credentials_manager = client_credentials_manager)
        return authenticated_spotify_api