'''
import json 
from flask import Flask
from flask_restful import Resource, Api, reqparse
from google.cloud import bigquery
import os

app = Flask(__name__)
api = Api(app)

# parameterized query- one is scalar and other is array 
class PrintUser(Resource):
    def get(self):
    
        client = bigquery.Client()
        query = """
                    SELECT
                        *
                    FROM
                    `adept-odyssey-314211.test_crypto_cf.test_prophet_global` 
                """

        query_res = client.query(query).result().to_dataframe()

        jsonfiles = json.loads(query_res.to_json(orient='records'))

        return jsonfiles

api.add_resource(PrintUser, '/')

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=int(os.environ.get('PORT',8080)))
'''
'''
import json 
from flask import Flask, Response
from flask_restful import Resource, Api, reqparse
from google.cloud import bigquery
import os

app = Flask(__name__)
api = Api(app)

# parameterized query- one is scalar and other is array 
class PrintUser(Resource):
    def get(self):
    
        client = bigquery.Client()
        query = """
                    SELECT
                        *
                    FROM
                    `adept-odyssey-314211.test_crypto_cf.test_prophet_global` 
                """

        query_res = client.query(query).result().to_dataframe()

        jsonfiles = json.loads(query_res.to_json(orient='records'))

        def generate(bytefile, chunksize=2 ** 20):
            i=0
            while (i+chunksize) < len(bytefile):
                yield bytefile[i:i+chunksize]
                i = i + chunksize
            
            yield bytefile[i:]

                #yield ",".join(repr(e) for e in list(row)) + '\n'

        #return jsonfiles
        return Response(generate(str.encode(json.dumps(jsonfiles), 'utf-8')),  mimetype='application/json')

api.add_resource(PrintUser, '/')

if __name__ == '__main__':
    #app.run(host='0.0.0.0',port=int(os.environ.get('PORT',12345)))
    app.run(host='0.0.0.0',port=int(os.environ.get('PORT',8080)))
'''
'''
import json 
from flask import Flask, Response
from flask_restful import Resource, Api, reqparse
from google.cloud import bigquery
import os

app = Flask(__name__)
api = Api(app)

from datetime import date, timedelta
date_max = date.today() - timedelta(days=18)

# argument parsing
parser = reqparse.RequestParser()
parser.add_argument('stat', choices = ('MAX', 'MIN', 'AVG'), default=None)
parser.add_argument('currencies', action = 'append')
parser.add_argument('start_date', default='2020-11-11 08:00:00 UTC')
parser.add_argument('end_date', default=date_max)

# parameterized query- one is scalar and other is array 
class PrintUser(Resource):
    def get(self):
        client = bigquery.Client()

        # parsing the input argument
        args = parser.parse_args()
        stat = args['stat'] 
        column = '(y)'
        start_date = args['start_date']
        end_date = args['end_date']

        if args['currencies'] is not None:
            currencies = tuple(args['currencies'])
        else:
            currencies = None 

        #print(currencies)

        if stat is None: 
            if currencies is None:  
                query = " \
                SELECT \
                    *   \
                FROM     \
                    `adept-odyssey-314211.test_crypto_cf.test_prophet_global` \
                    WHERE ds BETWEEN '{start_date}' AND '{end_date}' \
                ".format(start_date=start_date, end_date=end_date)

            else:
                query = " \
                SELECT \
                    *   \
                FROM     \
                    `adept-odyssey-314211.test_crypto_cf.test_prophet_global` \
                    WHERE symbol in {currencies} AND ds BETWEEN '{start_date}' AND '{end_date}' \
                ".format(currencies=currencies, start_date=start_date, end_date=end_date)

        else:
            if currencies is None:   
                query = " \
                    SELECT \
                        symbol, {stat_column} as {stat} \
                    FROM \
                        `adept-odyssey-314211.test_crypto_cf.test_prophet_global` \
                    WHERE ds BETWEEN '{start_date}' AND '{end_date}' \
                    GROUP BY symbol \
                    ORDER BY symbol \
                ".format(stat_column=stat+column,stat=stat, 
                        start_date=start_date, end_date=end_date)

            else:
                query = " \
                    SELECT \
                        symbol, {stat_column} as {stat} \
                    FROM \
                        `adept-odyssey-314211.test_crypto_cf.test_prophet_global` \
                    WHERE symbol in {currencies} \
                    AND ds BETWEEN '{start_date}' AND '{end_date}' \
                    GROUP BY symbol \
                    ORDER BY symbol \
                ".format(stat_column=stat+column,stat=stat, currencies=currencies, 
                        start_date=start_date, end_date=end_date)

        print(query)

        query_res = client.query(query).result().to_dataframe()

        jsonfiles = json.loads(query_res.to_json(orient='records'))

        def generate(bytefile, chunksize=2 ** 20): #1MB
            i=0
            while (i+chunksize) < len(bytefile):
                yield bytefile[i:i+chunksize]
                i = i + chunksize
            
            yield bytefile[i:]

                #yield ",".join(repr(e) for e in list(row)) + '\n'

        #return jsonfiles
        return Response(generate(str.encode(json.dumps(jsonfiles), 'utf-8')),  mimetype='application/json')

api.add_resource(PrintUser, '/')

if __name__ == '__main__':
    #app.run(host='0.0.0.0',port=int(os.environ.get('PORT',12345)))
    app.run(host='0.0.0.0',port=int(os.environ.get('PORT',8080)))
'''

import json 
from flask import Flask, Response
from flask_restful import Resource, Api, reqparse
from google.cloud import bigquery
import os

app = Flask(__name__)
api = Api(app)

from datetime import date, timedelta
date_max = date.today() - timedelta(days=18)

# argument parsing
parser = reqparse.RequestParser()
parser.add_argument('stat', choices = ('MAX', 'MIN', 'AVG'), default=None)
parser.add_argument('currencies', action = 'append')
parser.add_argument('start_date', default='2020-11-11 08:00:00 UTC')
parser.add_argument('end_date', default=date_max)
parser.add_argument('model', choices = ('ARIMA', 'PROPHET'), default='PROPHET')

# parameterized query- one is scalar and other is array 
class PrintUser(Resource):
    def get(self):
        client = bigquery.Client()

        # parsing the input argument
        args = parser.parse_args()
        stat = args['stat'] 
        start_date = args['start_date']
        end_date = args['end_date']
        model = args['model']

        if args['currencies'] is not None:
            currencies = tuple(args['currencies'])
        else:
            currencies = None 

        if model == 'PROPHET':
            column = '(y)'
            if stat is None: 
                if currencies is None:  

                    query =""" 
                           SELECT p.symbol, 
                           p.ds,     
                           FORMAT_TIMESTAMP('%Y%m%d%H%M%S', p.ds) AS date_str, 
                           p.y,        
                           p.yhat,    
                           p.yhat_lower, 
                           p.yhat_upper,  
                           b.intrp         
                           FROM             
                           `temp_crypto_batch.test_prophet_global` p 
                           LEFT JOIN 
                            (SELECT 
                            * 
                            FROM 
                            `temp_crypto_batch.batch_hour_interpo` 
                            WHERE tumble > (SELECT TIMESTAMP_SUB(max(ds), INTERVAL 5 DAY) 
                            FROM `temp_crypto_batch.test_prophet_global`) 
                            AND tumble <= 
                            (SELECT max(ds) FROM `temp_crypto_batch.test_prophet_global`) 
                            ) b 
                            ON 
                            b.tumble = p.ds AND p.symbol = b.symbol 
                            WHERE ds BETWEEN '{start_date}' AND '{end_date}' 
                            """.format(start_date=start_date, end_date=end_date)
                            
                else:

                    query =""" 
                           SELECT p.symbol, 
                           p.ds,     
                           FORMAT_TIMESTAMP('%Y%m%d%H%M%S', p.ds) AS date_str, 
                           p.y,        
                           p.yhat,    
                           p.yhat_lower, 
                           p.yhat_upper,  
                           b.intrp         
                           FROM             
                           `temp_crypto_batch.test_prophet_global` p 
                           LEFT JOIN 
                            (SELECT 
                            * 
                            FROM 
                            `temp_crypto_batch.batch_hour_interpo` 
                            WHERE tumble > (SELECT TIMESTAMP_SUB(max(ds), INTERVAL 5 DAY) 
                            FROM `temp_crypto_batch.test_prophet_global`) 
                            AND tumble <= 
                            (SELECT max(ds) FROM `temp_crypto_batch.test_prophet_global`) 
                            ) b 
                            ON 
                            b.tumble = p.ds AND p.symbol = b.symbol 
                            WHERE p.symbol in {currencies} AND ds BETWEEN '{start_date}' AND '{end_date}' 
                            """.format(currencies=currencies, start_date=start_date, end_date=end_date)
            else:
                if currencies is None: 
                    query =""" 
                           SELECT p.symbol, {stat_column} as {stat}          
                           FROM             
                           `temp_crypto_batch.test_prophet_global` p 
                           LEFT JOIN 
                            (SELECT 
                            * 
                            FROM 
                            `temp_crypto_batch.batch_hour_interpo` 
                            WHERE tumble > (SELECT TIMESTAMP_SUB(max(ds), INTERVAL 5 DAY) 
                            FROM `temp_crypto_batch.test_prophet_global`) 
                            AND tumble <= 
                            (SELECT max(ds) FROM `temp_crypto_batch.test_prophet_global`) 
                            ) b 
                            ON 
                            b.tumble = p.ds AND p.symbol = b.symbol 
                            WHERE ds BETWEEN '{start_date}' AND '{end_date}' 
                            GROUP BY symbol
                            ORDER BY symbol 
                            """.format(stat_column=stat+column,stat=stat,start_date=start_date, end_date=end_date)
                            
                else:
                    query =""" 
                           SELECT p.symbol, {stat_column} as {stat}          
                           FROM             
                           `temp_crypto_batch.test_prophet_global` p 
                           LEFT JOIN 
                            (SELECT 
                            * 
                            FROM 
                            `temp_crypto_batch.batch_hour_interpo` 
                            WHERE tumble > (SELECT TIMESTAMP_SUB(max(ds), INTERVAL 5 DAY) 
                            FROM `temp_crypto_batch.test_prophet_global`) 
                            AND tumble <= 
                            (SELECT max(ds) FROM `temp_crypto_batch.test_prophet_global`) 
                            ) b 
                            ON 
                            b.tumble = p.ds AND p.symbol = b.symbol 
                            WHERE p.symbol in {currencies} AND ds BETWEEN '{start_date}' AND '{end_date}'
                            GROUP BY symbol
                            ORDER BY symbol 
                            """.format(stat_column=stat+column,stat=stat,currencies=currencies,
                            start_date=start_date, end_date=end_date)

        elif model == 'ARIMA':
            column = '(time_series_data)'
            if stat is None: 
                if currencies is None:  

                    query =""" 
                           SELECT
                           a.symbol, 
                           time_series_timestamp, 
                           FORMAT_TIMESTAMP('%Y%m%d%H%M%S', time_series_timestamp) AS date_str, 
                           if (time_series_timestamp <= (SELECT TIMESTAMP_SUB(max(time_series_timestamp), INTERVAL 5 DAY) 
                           FROM 
                           ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                           STRUCT(120 AS horizon, 0.8 AS confidence_level))
                            ), time_series_data, NULL) as y, 
                           if (time_series_timestamp > (SELECT TIMESTAMP_SUB(max(time_series_timestamp), INTERVAL 5 DAY) 
                           FROM 
                           ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                           STRUCT(120 AS horizon, 0.8 AS confidence_level))
                            ), time_series_data, NULL) as yhat,
                           prediction_interval_lower_bound as yhat_lower, 
                           prediction_interval_upper_bound as yhat_upper, 
                           b.intrp
                           FROM
                           ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                           STRUCT(120 AS horizon, 0.8 AS confidence_level)) a
                           LEFT JOIN
                           (SELECT 
                           *
                           FROM  
                           `temp_crypto_batch.batch_hour_interpo`
                           WHERE tumble > (SELECT TIMESTAMP_SUB(max(time_series_timestamp), INTERVAL 5 DAY) 
                           FROM 
                           ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                           STRUCT(120 AS horizon, 0.8 AS confidence_level))
                           )
                           AND tumble <= (SELECT max(time_series_timestamp) 
                           FROM 
                           ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                           STRUCT(120 AS horizon, 0.8 AS confidence_level))
                           )
                           ) b
                           ON
                           b.tumble = a.time_series_timestamp AND a.symbol = b.symbol
                           WHERE time_series_timestamp BETWEEN '{start_date}' AND '{end_date}' 
                            """.format(start_date=start_date, end_date=end_date)
            
                else:

                    query =""" 
                           SELECT
                           a.symbol, 
                           time_series_timestamp, 
                           FORMAT_TIMESTAMP('%Y%m%d%H%M%S', time_series_timestamp) AS date_str, 
                           if (time_series_timestamp <= (SELECT TIMESTAMP_SUB(max(time_series_timestamp), INTERVAL 5 DAY) 
                           FROM 
                           ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                           STRUCT(120 AS horizon, 0.8 AS confidence_level))
                            ), time_series_data, NULL) as y, 
                           if (time_series_timestamp > (SELECT TIMESTAMP_SUB(max(time_series_timestamp), INTERVAL 5 DAY) 
                           FROM 
                           ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                           STRUCT(120 AS horizon, 0.8 AS confidence_level))
                            ), time_series_data, NULL) as yhat,
                           prediction_interval_lower_bound as yhat_lower, 
                           prediction_interval_upper_bound as yhat_upper, 
                           b.intrp
                           FROM
                           ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                           STRUCT(120 AS horizon, 0.8 AS confidence_level)) a
                           LEFT JOIN
                           (SELECT 
                           *
                           FROM  
                           `temp_crypto_batch.batch_hour_interpo`
                           WHERE tumble > (SELECT TIMESTAMP_SUB(max(time_series_timestamp), INTERVAL 5 DAY) 
                           FROM 
                           ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                           STRUCT(120 AS horizon, 0.8 AS confidence_level))
                           )
                           AND tumble <= (SELECT max(time_series_timestamp) 
                           FROM 
                           ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                           STRUCT(120 AS horizon, 0.8 AS confidence_level))
                           )
                           ) b
                           ON
                           b.tumble = a.time_series_timestamp AND a.symbol = b.symbol
                           WHERE a.symbol in {currencies} AND time_series_timestamp BETWEEN '{start_date}' AND '{end_date}' 
                            """.format(currencies=currencies, start_date=start_date, end_date=end_date)

            else:
                if currencies is None: 

                    query =""" 
                           SELECT a.symbol, {stat_column} as {stat}
                           FROM
                           ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                           STRUCT(120 AS horizon, 0.8 AS confidence_level)) a
                           LEFT JOIN
                           (SELECT 
                           *
                           FROM  
                           `temp_crypto_batch.batch_hour_interpo`
                           WHERE tumble > (SELECT TIMESTAMP_SUB(max(time_series_timestamp), INTERVAL 5 DAY) 
                           FROM 
                           ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                           STRUCT(120 AS horizon, 0.8 AS confidence_level))
                           )
                           AND tumble <= (SELECT max(time_series_timestamp) 
                           FROM 
                           ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                           STRUCT(120 AS horizon, 0.8 AS confidence_level))
                           )
                           ) b
                           ON
                           b.tumble = a.time_series_timestamp AND a.symbol = b.symbol
                           WHERE time_series_timestamp BETWEEN '{start_date}' AND '{end_date}' 
                           GROUP BY symbol
                           ORDER BY symbol 
                            """.format(stat_column=stat+column,stat=stat,start_date=start_date, end_date=end_date)
                            
                else:

                    query =""" 
                           SELECT a.symbol, {stat_column} as {stat}
                           FROM
                           ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                           STRUCT(120 AS horizon, 0.8 AS confidence_level)) a
                           LEFT JOIN
                           (SELECT 
                           *
                           FROM  
                           `temp_crypto_batch.batch_hour_interpo`
                           WHERE tumble > (SELECT TIMESTAMP_SUB(max(time_series_timestamp), INTERVAL 5 DAY) 
                           FROM 
                           ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                           STRUCT(120 AS horizon, 0.8 AS confidence_level))
                           )
                           AND tumble <= (SELECT max(time_series_timestamp) 
                           FROM 
                           ML.EXPLAIN_FORECAST(MODEL timeseries.arima_model_interpo_hour_global,
                           STRUCT(120 AS horizon, 0.8 AS confidence_level))
                           )
                           ) b
                           ON
                           b.tumble = a.time_series_timestamp AND a.symbol = b.symbol
                           WHERE a.symbol in {currencies} AND time_series_timestamp BETWEEN '{start_date}' AND '{end_date}' 
                           GROUP BY symbol
                           ORDER BY symbol 
                            """.format(stat_column=stat+column,stat=stat,currencies=currencies,
                            start_date=start_date, end_date=end_date)  
        else: 
            print("this model is not defined. Please choose another one.")
            return
            
        #print(query)

        #return

        query_res = client.query(query).result().to_dataframe()

        jsonfiles = json.loads(query_res.to_json(orient='records'))

        def generate(bytefile, chunksize=2 ** 20): #1MB
            i=0
            while (i+chunksize) < len(bytefile):
                yield bytefile[i:i+chunksize]
                i = i + chunksize
            
            yield bytefile[i:]

                #yield ",".join(repr(e) for e in list(row)) + '\n'

        #return jsonfiles
        return Response(generate(str.encode(json.dumps(jsonfiles), 'utf-8')),  mimetype='application/json')

api.add_resource(PrintUser, '/')

if __name__ == '__main__':
    #app.run(host='0.0.0.0',port=int(os.environ.get('PORT',12345)))
    app.run(host='0.0.0.0',port=int(os.environ.get('PORT',8080)))
