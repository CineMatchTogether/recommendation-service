import pandas as pd
import logging
from config import Config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

class DataLoader:
    def __init__(self, movies_path=Config.MOVIES_PATH, ratings_path=Config.RATINGS_PATH):
        self.movies_path = movies_path
        self.ratings_path = ratings_path
        self.movies_df = None
        self.ratings_df = None
        self.user_item_matrix = None
    
    def load_data(self):
        """Load and preprocess movie and rating data."""
        try:
            # Load movies
            self.movies_df = pd.read_csv(self.movies_path)
            self.movies_df.dropna(subset=['title', 'genres'], inplace=True)
            self.movies_df['genres'] = self.movies_df['genres'].str.replace('|', ' ')
            self.movies_df['content'] = self.movies_df['title'] + ' ' + self.movies_df['genres']

            self.dbid_to_movieid = dict(zip(self.movies_df['db_id'], self.movies_df['movieId']))
            logging.info("Movies data loaded and preprocessed")
            
            # Load ratings
            self.ratings_df = pd.read_csv(self.ratings_path)
            logging.info("Ratings data loaded")
            
            # Create user-item matrix
            self.user_item_matrix = self.ratings_df.pivot(index='userId', 
                                                        columns='movieId', 
                                                        values='rating').fillna(0)
            logging.info("User-item matrix created")
            
            return self.movies_df, self.ratings_df, self.user_item_matrix
        
        except FileNotFoundError as e:
            logging.error(f"Data file not found: {e}")
            raise
        except Exception as e:
            logging.error(f"Error loading data: {e}")
            raise
    
    def validate_movie_ids(self, movie_ids):
        """Validate if movie IDs exist in the dataset."""
        if self.movies_df is None:
            raise ValueError("Movies data not loaded")
        invalid_ids = [mid for mid in movie_ids if mid not in self.movies_df['movieId'].values]
        if invalid_ids:
            raise ValueError(f"Invalid movie IDs: {invalid_ids}")