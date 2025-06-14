import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import logging
import joblib
import os
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

class ContentBasedFilter:
    def __init__(self, movies_df, cache_dir='cache'):
        self.movies_df = movies_df
        self.tfidf_matrix = None
        self.content_similarity_df = None
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self._initialize()
    
    def _initialize(self):
        """Initialize TF-IDF matrix and content similarity with caching."""
        try:
            cache_file = self.cache_dir / 'tfidf_matrix.joblib'
            similarity_cache_file = self.cache_dir / 'content_similarity.joblib'
            
            # Try to load from cache
            if cache_file.exists() and similarity_cache_file.exists():
                logging.info("Loading TF-IDF matrix and similarity from cache")
                self.tfidf_matrix = joblib.load(cache_file)
                self.content_similarity_df = joblib.load(similarity_cache_file)
            else:
                logging.info("Computing TF-IDF matrix and similarity")
                tfidf = TfidfVectorizer(stop_words='english')
                self.tfidf_matrix = tfidf.fit_transform(self.movies_df['content'])
                content_similarity = cosine_similarity(self.tfidf_matrix)
                self.content_similarity_df = pd.DataFrame(content_similarity, 
                                                       index=self.movies_df['movieId'], 
                                                       columns=self.movies_df['movieId'])
                
                # Save to cache
                joblib.dump(self.tfidf_matrix, cache_file)
                joblib.dump(self.content_similarity_df, similarity_cache_file)
                logging.info("TF-IDF matrix and similarity saved to cache")
                
        except Exception as e:
            logging.error(f"Error initializing content-based filter: {e}")
            raise
    
    def get_recommendations(self, watched_movies, top_n):
        """Get content-based recommendations for watched movies."""
        try:
            scores = pd.Series(0.0, index=self.movies_df['movieId'])
            for movie_id in watched_movies:
                if movie_id in self.content_similarity_df.index:
                    similar_scores = self.content_similarity_df[movie_id]
                    scores = scores.add(similar_scores, fill_value=0)
            return scores.sort_values(ascending=False).head(top_n).index
        except Exception as e:
            logging.error(f"Error in content-based recommendations: {e}")
            raise