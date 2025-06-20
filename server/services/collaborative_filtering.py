import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import logging
import os
import pickle
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

class CollaborativeFilter:
    def __init__(self, user_item_matrix):
        self.user_item_matrix = user_item_matrix
        self.item_similarity_df = None
        self.user_similarity_df = None
        self._initialize()
    
    def _initialize(self):
        """Initialize item and user similarity matrices."""
        try:
            # Try to load existing matrices
            if self._load_matrices():
                logging.info("Successfully loaded existing similarity matrices")
                return

            # If loading failed, compute matrices
            # Item-based similarity
            item_similarity = cosine_similarity(self.user_item_matrix.T)
            self.item_similarity_df = pd.DataFrame(item_similarity, 
                                                  index=self.user_item_matrix.columns, 
                                                  columns=self.user_item_matrix.columns)
            logging.info("Item-based similarity matrix initialized")
            
            # User-based similarity
            user_similarity = cosine_similarity(self.user_item_matrix)
            self.user_similarity_df = pd.DataFrame(user_similarity, 
                                                 index=self.user_item_matrix.index, 
                                                 columns=self.user_item_matrix.index)
            logging.info("User-based similarity matrix initialized")

            # Save computed matrices
            self._save_matrices()
        
        except Exception as e:
            logging.error(f"Error initializing collaborative filter: {e}")
            raise

    def _save_matrices(self):
        """Save similarity matrices to files."""
        try:
            # Create directory if it doesn't exist
            cache_dir = Path("cache")
            cache_dir.mkdir(exist_ok=True)

            # Save matrices
            with open(cache_dir / "item_similarity.pkl", "wb") as f:
                pickle.dump(self.item_similarity_df, f)
            with open(cache_dir / "user_similarity.pkl", "wb") as f:
                pickle.dump(self.user_similarity_df, f)
            logging.info("Similarity matrices saved successfully")
        except Exception as e:
            logging.error(f"Error saving matrices: {e}")

    def _load_matrices(self):
        """Load similarity matrices from files."""
        try:
            cache_dir = Path("cache")
            item_similarity_path = cache_dir / "item_similarity.pkl"
            user_similarity_path = cache_dir / "user_similarity.pkl"

            if not (item_similarity_path.exists() and user_similarity_path.exists()):
                return False

            with open(item_similarity_path, "rb") as f:
                self.item_similarity_df = pickle.load(f)
            with open(user_similarity_path, "rb") as f:
                self.user_similarity_df = pickle.load(f)
            return True
        except Exception as e:
            logging.error(f"Error loading matrices: {e}")
            return False
    
    def item_based_recommendations(self, watched_movies, top_n):
        """Get item-based collaborative filtering recommendations."""
        try:
            scores = pd.Series(0.0, index=self.user_item_matrix.columns)
            for item in watched_movies:
                if item in self.item_similarity_df.index:
                    similar_scores = self.item_similarity_df[item]
                    scores = scores.add(similar_scores, fill_value=0)
            return scores.sort_values(ascending=False).head(top_n).index
        except Exception as e:
            logging.error(f"Error in item-based recommendations: {e}")
            raise
    
    def user_based_recommendations(self, user_ids, top_n):
        """Get user-based collaborative filtering recommendations."""
        try:
            recommendations = pd.Series(dtype=float)
            for user_id in user_ids:
                if user_id in self.user_similarity_df.index:
                    similar_users = self.user_similarity_df[user_id].sort_values(ascending=False)[1:11]
                    for sim_user, similarity in similar_users.items():
                        user_ratings = self.user_item_matrix.loc[sim_user]
                        rated_items = user_ratings[user_ratings > 0]
                        recommendations = recommendations.add(rated_items * similarity, fill_value=0)
            return recommendations.sort_values(ascending=False).head(top_n).index
        except Exception as e:
            logging.error(f"Error in user-based recommendations: {e}")
            raise