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
        response['result'] = prog_reader.generate_prognose(topic)

        return response
    except Exception as e:
        return jsonify({'error here': e.__str__()})
