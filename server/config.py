# Configuration settings for the movie recommendation system
import os

class Config:
    # File paths
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    MOVIES_PATH = os.path.join(BASE_DIR, 'movies_large_updated.csv')
    RATINGS_PATH = os.path.join(BASE_DIR, 'ratings_large_updated.csv')
    
    # Weights for hybrid filtering
    WEIGHTS = {
        'item_based': 0.2,
        'user_based': 0.2,
        'content_based': 0.6
    }
    
    # Default number of recommendations
    DEFAULT_TOP_N = 50