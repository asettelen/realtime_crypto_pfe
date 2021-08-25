import logging
import re
import json
import time
import traceback
import google.auth

import apache_beam as beam
from apache_beam import WithKeys, GroupByKey
from apache_beam import FlatMap, Map, ParDo, Flatten, Filter, GroupBy
from apache_beam import Values, CombineGlobally, CombinePerKey
from apache_beam import pvalue, window, WindowInto
from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.combiners import Top, Mean, Count
from apache_beam.io.textio import ReadFromText, WriteToText
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery

from apache_beam.runners import DataflowRunner
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import apache_beam.runners.interactive.interactive_beam as ib

from apache_beam.testing.util import assert_that, is_empty, equal_to

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

query = """
SELECT
  *
FROM
  adept-odyssey-314211.test_crypto_ml.batch_hour_interpo
WHERE
  symbol = 'ETH'
ORDER BY 
  tumble ASC
"""

class KeepDoFn(beam.DoFn):
    def process(self, element):
        return [{
            'symbol': element['symbol'],
            'ds': element['tumble'],
            'y' : element['intrp']
    }]

###########################
    
class KeepDoFnAfterGrouping(beam.DoFn):
    def process(self, element):
        return [{
            'symbol': element[0],
            'data': element[1]
    }]
    
###########################

class Prophet_M(beam.DoFn):
    def process(self, element):
        import pandas
        #from datetime import datetime
        import numpy as np
        from prophet import Prophet
        
        symbol = element['symbol']
        data = element['data']
        
        data = pandas.DataFrame(data)
        data['ds'] = data['ds'].dt.tz_localize(None)
        
        ##############
        
        max_datetime = max(data['ds'])
        # Extract training data
        train_data = data[data['ds'] <= max_datetime]
        
        #############
        
        m = Prophet(seasonality_mode="multiplicative") 
            
        #############
        
        """Fit the model using the training data."""
        m.fit(train_data)
        
        ############
        
        future = m.make_future_dataframe(periods=120, freq='H')      
        forecast = m.predict(future)
        
        ###########
        
        forecast=pandas.merge(forecast, train_data, on='ds', how='left')
        
        x = np.array(symbol)
        forecast['symbol']=np.repeat(x, len(forecast), axis=0)
        
        ############
        
        forecast_dict = forecast.to_dict('records')
        
        return [{
        'symbol' : symbol,
        'forecast': forecast_dict   
        }]
    
########################

class Expand_Predictions_M(beam.DoFn):
    def process(self, element):
        import pandas, pytz, math
        
        return [{
        'symbol' : element['symbol'],
        'ds': pytz.utc.localize(element['ds']).to_pydatetime(),
        'y': element['y'] if not(math.isnan(element['y'])) else None,
        'yhat': element['yhat'],
        'yhat_lower': element['yhat_lower'], 
        'yhat_upper': element['yhat_upper']
        }]
    
#######################

project='adept-odyssey-314211'
table = "{}:test_crypto_cf.test_prophet_eth_10".format(project)
schema = "symbol:STRING, ds:TIMESTAMP, y:FLOAT, yhat:FLOAT, yhat_lower:FLOAT, yhat_upper:FLOAT"
        
logging.getLogger().setLevel(logging.INFO)

project = google.auth.default()[1]
bucket = "gs://temp_crypto"

pipeline_parameters = [
    '--project', project,
    '--staging_location', "%s/staging_bq_notebook" % bucket,
    '--temp_location', "%s/temp_bq_notebook" % bucket,
    "--setup_file", './setup.py', 
    "--region", "us-central1",
]

with beam.Pipeline("DataFlowRunner", argv=pipeline_parameters) as p:
    create = (p | "Create" >>  beam.io.ReadFromBigQuery(query=query,use_standard_sql=True)
                | "Map Keys" >> beam.ParDo(KeepDoFn())
                | "Add Keys" >> WithKeys(lambda x: x["symbol"])
                | GroupByKey()
                | "Rename After Grouping" >> beam.ParDo(KeepDoFnAfterGrouping())
                | "Prophet Data" >> beam.ParDo(Prophet_M())
                | "Flat Map Data" >> FlatMap(lambda x: x["forecast"])
                | "Expand Predictions" >> beam.ParDo(Expand_Predictions_M())
                | "Write To BigQuery" >> WriteToBigQuery(table=table, schema=schema,
                                  create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                                  write_disposition=BigQueryDisposition.WRITE_APPEND))
    p.run()
