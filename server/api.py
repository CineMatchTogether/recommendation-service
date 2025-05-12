from flask import Flask, request, jsonify
from flasgger import Swagger, swag_from
from marshmallow import Schema, fields, validate, ValidationError
from services.data_loader import DataLoader
from services.content_based import ContentBasedFilter
from services.collaborative_filtering import CollaborativeFilter
from services.hybrid_recommender import HybridRecommender
import logging

# Configure logging
logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Initialize Flask and Swagger
app = Flask(__name__)
swagger = Swagger(app)

# Input schema using Marshmallow
class WeightsSchema(Schema):
    item_based = fields.Float(missing=0.2)
    user_based = fields.Float(missing=0.2)
    content_based = fields.Float(missing=0.6)

class RecommendInputSchema(Schema):
    watched_movies = fields.List(fields.List(fields.Int()), required=True)
    top_n = fields.Int(missing=20, validate=validate.Range(min=1))
    weights = fields.Nested(WeightsSchema, missing=None)

input_schema = RecommendInputSchema()

# Initialize recommendation system
try:
    data_loader = DataLoader()
    movies_df, ratings_df, user_item_matrix = data_loader.load_data()

    content_filter = ContentBasedFilter(movies_df)
    collaborative_filter = CollaborativeFilter(user_item_matrix)
    recommender = HybridRecommender(movies_df, content_filter, collaborative_filter)
except Exception as e:
    logging.error(f"Failed to initialize recommendation system: {e}")
    raise

@app.route('/recommend/group', methods=['POST'])
@swag_from({
    'tags': ['Recommendations'],
    'consumes': ['application/json'],
    'parameters': [
        {
            'name': 'body',
            'in': 'body',
            'required': True,
            'schema': {
                'type': 'object',
                'properties': {
                    'watched_movies': {
                        'type': 'array',
                        'items': {
                            'type': 'array',
                            'items': {'type': 'integer'}
                        }
                    },
                    'top_n': {
                        'type': 'integer',
                        'default': 20
                    },
                    'weights': {
                        'type': 'object',
                        'properties': {
                            'item_based': {'type': 'number', 'default': 0.2},
                            'user_based': {'type': 'number', 'default': 0.2},
                            'content_based': {'type': 'number', 'default': 0.6}
                        }
                    }
                },
                'required': ['watched_movies']
            }
        }
    ],
    'responses': {
        200: {
            'description': 'List of recommended movies',
            'examples': {
                'application/json': {
                    'recommendations': [
                        {'movieId': 1, 'title': 'Toy Story'},
                        {'movieId': 2, 'title': 'Jumanji'}
                    ],
                    'count': 2
                }
            }
        },
        400: {'description': 'Validation error'},
        500: {'description': 'Internal server error'}
    }
})
def group_recommendation():
    try:
        data = request.get_json()
        logging.info(f"Received request: {data}")

        # Validate input
        validated = input_schema.load(data)

        watched_movies_groups = validated['watched_movies']
        top_n = validated['top_n']
        weights = validated.get('weights')

        # Получаем маппинг db_id -> movieId
        dbid_to_movieid = data_loader.dbid_to_movieid

        # Конвертируем db_id -> movieId для всех групп
        movieid_groups = []
        all_watched_movie_ids = set()

        for group in watched_movies_groups:
            converted_group = [
                dbid_to_movieid[dbid]
                for dbid in group
                if dbid in dbid_to_movieid
            ]
            movieid_groups.append(converted_group)
            all_watched_movie_ids.update(converted_group)
        
        logging.info(len(all_watched_movie_ids))

        # Валидируем существование movieId
        data_loader.validate_movie_ids(all_watched_movie_ids)

        # Извлекаем user_ids по всем фильмам
        user_ids = []
        for group in movieid_groups:
            group_user_ids = ratings_df[ratings_df['movieId'].isin(group)]['userId'].unique()
            user_ids.extend(group_user_ids)

        # Получаем рекомендации
        recommendations = recommender.get_recommendations(user_ids, all_watched_movie_ids, top_n, weights)

        logging.info(f"Returning {len(recommendations)} recommendations")
        return jsonify({
            'recommendations': recommendations,
            'count': len(recommendations)
        })

    except ValidationError as ve:
        logging.error(f"Validation error: {ve}")
        return jsonify({'error': ve.messages}), 400
    except ValueError as ve:
        logging.error(f"Invalid input: {ve}")
        return jsonify({'error': str(ve)}), 400
    except Exception as e:
        logging.error(f"Server error: {e}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8082)
