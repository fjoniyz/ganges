from app import app
from flask import jsonify, request
import numpy as np

@app.route('/prognose')

def prognose():
    try:
        topic = request.args.get('topic', type=str, default='test')
        response = {}
        
        # TODO
        response['prognose_result'] = [topic]

        response = jsonify(response)
        response.headers.add('Access-Control-Allow-Origin', '*')

        return response
    except Exception as e:
        return jsonify({'error': e.__str__()})
