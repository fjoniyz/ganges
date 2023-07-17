from app import app
from flask import jsonify, request
import numpy as np

import app.prog_reader as prog_reader

@app.route('/prognose')

def handle_prognose():
    try:
        topic = request.args.get('topic', type=str, default='test')
        response = {}
        
        # TODO
        response['prognose_result'] = prog_reader.generate_prognose()

        response = jsonify(response)
        response.headers.add('Access-Control-Allow-Origin', '*')

        return response
    except Exception as e:
        return jsonify({'error': e.__str__()})
