from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from config import Config
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

class HybridRecommender:
    def __init__(self, movies_df, content_filter, collaborative_filter, weights=Config.WEIGHTS):
        self.movies_df = movies_df
        self.content_filter = content_filter
        self.collaborative_filter = collaborative_filter
        self.weights = weights
    
    def validate_weights(self):
        """Validate that weights sum to approximately 1."""
        total = sum(self.weights.values())
        if abs(total - 1.0) > 0.01:
            raise ValueError(f"Weights must sum to 1, got {total}")
    
    def get_recommendations(self, user_ids, watched_movies, top_n=Config.DEFAULT_TOP_N, weights=Config.WEIGHTS):
        """Get hybrid recommendations combining content and collaborative filtering."""
        if not user_ids or not watched_movies:
            raise ValueError("user_ids and watched_movies must not be empty.")
        if weights:
            self.weights = weights
        if not all(k in self.weights for k in ['item_based', 'user_based', 'content_based']) or not abs(sum(self.weights.values()) - 1.0) < 1e-6:
            raise ValueError("Weights must include 'item_based', 'user_based', 'content fostering' and sum to 1.")
    
        try:
            # Параллельное получение рекомендаций
            with ThreadPoolExecutor() as executor:
                futures = [
                    executor.submit(self.collaborative_filter.item_based_recommendations, watched_movies, top_n * 3),
                    executor.submit(self.collaborative_filter.user_based_recommendations, user_ids, top_n * 3),
                    executor.submit(self.content_filter.get_recommendations, watched_movies, top_n * 3)
                ]
                item_based_recs, user_based_recs, content_based_recs = [f.result() for f in futures]
    
            # Объединение рекомендаций
            movie_scores = defaultdict(float)
            recs = [
                (item_based_recs, self.weights['item_based']),
                (user_based_recs, self.weights['user_based']),
                (content_based_recs, self.weights['content_based'])
            ]
    
            for rec_list, weight in recs:
                for idx, movie_id in enumerate(rec_list):
                    movie_scores[movie_id] += weight * (1 / (1 + idx))
    
            movie_scores = {movie_id: score for movie_id, score in movie_scores.items() if movie_id not in watched_movies}
    
            # Сортировка и выбор топ-N
            recommended_ids = [
                movie_id for movie_id, _ in sorted(movie_scores.items(), key=lambda x: x[1], reverse=True)[:top_n]
            ]
    
            # Фильтрация и возврат результата
            filtered_df = self.movies_df[
                self.movies_df['movieId'].isin(recommended_ids)
            ][['movieId', 'title', 'db_id']].dropna(subset=['db_id']).rename(columns={'db_id': 'movieId'})[['movieId', 'title']]
    
            return filtered_df.to_dict('records')
    
        except Exception as e:
            logging.error(f"Error in hybrid recommendations: {e}")
            raise