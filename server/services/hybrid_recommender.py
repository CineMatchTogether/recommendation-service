from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from config import Config
import logging
import random

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
        """Get hybrid recommendations by randomly sampling movies with weights, or popular movies if watched_movies is empty."""

        if weights:
            self.weights = weights
        if not all(k in self.weights for k in ['item_based', 'user_based', 'content_based']) or not abs(sum(self.weights.values()) - 1.0) < 1e-6:
            raise ValueError("Weights must include 'item_based', 'user_based', 'content_based' and sum to 1.")

        try:
            if watched_movies:
                # Parallel retrieval of recommendations when watched_movies is non-empty
                with ThreadPoolExecutor() as executor:
                    futures = [
                        executor.submit(self.collaborative_filter.item_based_recommendations, watched_movies, top_n * 3),
                        executor.submit(self.collaborative_filter.user_based_recommendations, user_ids, top_n * 3),
                        executor.submit(self.content_filter.get_recommendations, watched_movies, top_n * 3)
                    ]
                    item_based_recs, user_based_recs, content_based_recs = [f.result() for f in futures]

                # Prepare recommendation sources and their weights
                rec_sources = [
                    ('item_based', item_based_recs, self.weights['item_based']),
                    ('user_based', user_based_recs, self.weights['user_based']),
                    ('content_based', content_based_recs, self.weights['content_based'])
                ]

                logging.info(f"Recommendation sources: {[(source, len(recs)) for source, recs, _ in rec_sources]}")

                # Combine all unique movie IDs (movieId)
                all_movies = set()
                for _, rec_list, _ in rec_sources:
                    all_movies.update(rec_list)

                # Remove watched movies (movieId)
                all_movies.difference_update(watched_movies)

                # Create a list of (movie_id, source_weight) for weighted random sampling
                movie_weight_pairs = []
                for movie_id in all_movies:
                    # Sum weights of sources that recommended this movie
                    movie_weight = sum(weight for source, rec_list, weight in rec_sources if movie_id in rec_list)
                    movie_weight_pairs.append((movie_id, movie_weight))
            else:
                # Handle empty watched_movies: select popular movies
                logging.info("watched_movies is empty, selecting popular movies")
                
                # Select top 3*top_n movies by popularity (e.g., rating)
                if 'rating' in self.movies_df.columns:
                    popular_movies = self.movies_df.sort_values(by='rating', ascending=False).head(top_n * 3)
                    movie_ids = popular_movies['movieId'].tolist()
                    weights = popular_movies['rating'].tolist()
                else:
                    # Fallback: select top 3*top_n movies by movieId if no rating column
                    logging.warning("No 'rating' column in movies_df, using uniform weights")
                    popular_movies = self.movies_df.sort_values(by='movieId').head(top_n * 3)
                    movie_ids = popular_movies['movieId'].tolist()
                    weights = [1.0] * len(movie_ids)  # Uniform weights

                movie_weight_pairs = list(zip(movie_ids, weights))

            # Perform weighted random sampling
            if not movie_weight_pairs:
                logging.warning("No valid movies available for recommendation after filtering.")
                return []

            movie_ids, weights = zip(*movie_weight_pairs) if movie_weight_pairs else ([], [])
            recommended_ids = random.choices(
                movie_ids,
                weights=weights,
                k=min(top_n, len(movie_ids))  # Ensure we don't sample more than available
            )

            # Remove duplicates while preserving order using dict.fromkeys
            recommended_ids = list(dict.fromkeys(recommended_ids))

            # Filter and format the result, selecting db_id as movieId
            filtered_df = self.movies_df[
                self.movies_df['movieId'].isin(recommended_ids)
            ][['db_id']].dropna(subset=['db_id']).rename(columns={'db_id': 'movieId'})

            return filtered_df.to_dict('records')

        except Exception as e:
            logging.error(f"Error in hybrid recommendations: {e}")
            raise