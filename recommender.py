import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


class GroupMovieRecommender:
    def __init__(self, csv_path: str):
        self.movies_df = pd.read_csv(csv_path)
        self.movies_df.dropna(subset=['title', 'genres'], inplace=True)
        self.movies_df['content'] = self.movies_df['title'] + ' ' + self.movies_df['genres']
        
        self.vectorizer = TfidfVectorizer(stop_words='english')
        self.tfidf_matrix = self.vectorizer.fit_transform(self.movies_df['content'])
        self.similarity_matrix = cosine_similarity(self.tfidf_matrix)

    def recommend_for_group(self, group_watched: list[list[str]], top_n: int = 10) -> list[dict]:
        all_watched_titles = set()
        
        for user_list in group_watched:
            for title in user_list:
                matched = self.movies_df[self.movies_df['title'].str.contains(title, case=False, regex=False)]
                if not matched.empty:
                    all_watched_titles.update(matched['title'].values)

        if not all_watched_titles:
            return []

        watched_indices = self.movies_df[self.movies_df['title'].isin(all_watched_titles)].index
        group_similarity = self.similarity_matrix[watched_indices].sum(axis=0)

        unwatched_indices = self.movies_df[~self.movies_df['title'].isin(all_watched_titles)].index
        sorted_indices = group_similarity[unwatched_indices].argsort()[::-1][:top_n]
        recommended_indices = unwatched_indices[sorted_indices]

        return self.movies_df.iloc[recommended_indices][['movieId', 'title', 'genres']].to_dict(orient='records')
