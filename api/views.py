import os
import yaml
import cx_Oracle as oracle

from flask import request, current_app, jsonify, abort
from flask.views import MethodView

with open(os.path.join(os.path.dirname(__file__), '..', 'config.yml'), encoding='utf-8') as f:
    config = yaml.safe_load(f)


class TableCounter(MethodView):
    """
    View to provide a count of rows in a given table where the endpoint looks like this:
    /api/table-count?table=<<name_of_table>>
    Returns string of error or integer of row count
    """

    # Forbid any requests other than GET
    methods = ['GET', ]

    def get(self):
        """
        Return number of records in a table passed in args
        """
        data = request.args.to_dict()
        current_app.logger.info('Got request with args: {}'.format(data))
        if 'table' in [k.lower() for k in data.keys()]:
            count = self.count_table_rows(data['table'])
            return jsonify(count)
        else:
            # Abort if not 'table' argument was provided.
            abort(404)

    @staticmethod
    def count_table_rows(table):
        """
        Count the rows in a give table
        """

        # Get DB specific config dict and ensure the table name is the fully qualified version of it.
        db_config = config.get('DATABASE_CONFIG')
        if not table.lower().startswith(db_config['TABLE_PREFIX']):
            table = '{}.{}'.format(db_config['TABLE_PREFIX'], table)

        # Define the TNS and try to connect to DB
        dns_tns = '(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port})))' \
                  '(CONNECT_DATA=(SERVICE_NAME={service_name})))'.format(host=db_config['HOST'],
                                                                         port=db_config['PORT'],
                                                                         service_name=db_config['SERVICE_NAME']
                                                                         )
        try:
            db = oracle.connect(db_config['DB_USERNAME'], db_config['DB_PASSWORD'], dns_tns)
        except ConnectionError as exc:
            current_app.logger.exception('Failed to connect to database: {}'.format(exc))
            return 'N/A'

        # Get a cursor and count rows in table
        cursor = db.cursor()

        try:
            cursor.execute('SELECT COUNT(*) AS ROW_COUNT FROM {}'.format(table))
            result = cursor.fetchall()[0][0]
            return result
        except oracle.DatabaseError as exc:
            current_app.logger.exception('Failed to execute query, perhaps {} does not exist: {}'.format(table, exc))
            return 'N/A - Table might not exist: {}'.format(table)
        finally:
            cursor.close()
            db.close()
