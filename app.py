from flask import Flask, request, jsonify
from flask_cors import CORS
from flasgger import Swagger
from recommender import GroupMovieRecommender

app = Flask(__name__)
CORS(app)
swagger = Swagger(app)

recommender = GroupMovieRecommender("./movie.csv")


@app.route('/recommend/group', methods=['POST'])
def recommend_for_group():
    """
    Групповая рекомендация фильмов
    ---
    tags:
      - Recommendations
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          properties:
            group_watched:
              type: array
              items:
                type: array
                items:
                  type: string
              example: [["Toy Story (1995)", "Jumanji (1995)"], ["Grumpier Old Men (1995)"]]
            top_n:
              type: integer
              example: 5
    responses:
      200:
        description: Список рекомендованных фильмов
        schema:
          type: array
          items:
            type: object
            properties:
              movieId:
                type: integer
              title:
                type: string
              genres:
                type: string
    """
    data = request.get_json()
    group_watched = data.get('group_watched', [])
    top_n = data.get('top_n', 10)

    if not group_watched:
        return jsonify({"error": "group_watched is required"}), 400

    recommendations = recommender.recommend_for_group(group_watched, top_n)
    return jsonify(recommendations)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8082, debug=True)
