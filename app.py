import os
import yaml

from flask import Flask
from flask_cors import CORS

from api.views import TableCounter

# Create app and allow all CORS
app = Flask(__name__)
CORS(app)

# Load configuration
with open(os.path.join(os.path.dirname(__file__), 'config.yml'), encoding='utf-8') as f:
    config = yaml.safe_load(f)


@app.route('/')
def home():
    return 'hello world'

# Register the TableCounter view, counting rows in a table, request = /api/table-count?table=<<some table name>>
app.add_url_rule(rule='/api/table-count', view_func=TableCounter.as_view('table-counter'))

if __name__ == '__main__':

    app.run(host='0.0.0.0',
            port=config.get('SITE_CONFIG', {}).get('PORT', 5555),
            debug=False,
            threaded=True
            )
