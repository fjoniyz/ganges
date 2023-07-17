from app import app
from flask import jsonify, request
import matplotlib.pyplot as plt
import numpy as np

import numpy as np
import json


@app.route('/prognose')
def prognose():
    try:
        topic = request.args.get('topic', type=str, default='test')
        prognose_result = []
        
        # TODO

        return jsonify({'prognose_result': prognose_result})
    except Exception as e:
        return jsonify({'error': e.__str__()})
