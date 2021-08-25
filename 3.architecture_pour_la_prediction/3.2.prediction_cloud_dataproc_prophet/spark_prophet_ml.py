from fbprophet import Prophet
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, struct, to_timestamp
import pandas
from datetime import datetime
import pytz, math
from pyspark.sql.types import *
import google.auth

def transform_data_m(row):
    import pandas
    from datetime import datetime
    """Transform data from pyspark.sql.Row to python dict to be used in rdd."""
    data = row['data']
    symbol = row['symbol']
    
    # Transform [pyspark.sql.Dataframe.Row] -> [dict]
    data_dicts = []
    for d in data:
        data_dicts.append(d.asDict())

    # Convert into pandas dataframe for fbprophet
    data = pandas.DataFrame(data_dicts)
    
    data['ds'] = data['ds'].dt.tz_localize(None)

    return {
        'symbol' : symbol,
        'data': data,
    }

###########################

def partition_data_m(d):
    """Split data into training and testing based on timestamp."""
    # Extract data from pd.Dataframe
    data = d['data']

    # Find max timestamp and extract timestamp for start of day
    max_datetime = max(data['ds'])
    #start_datetime = max_datetime.replace(hour=00, minute=00, second=00)

    # Extract training data
    train_data = data[data['ds'] <= max_datetime]

    # Account for zeros in data while still applying uniform transform
    #train_data['y'] = train_data['y'].apply(lambda x: np.log(x + 1))

    # Assign train/test split
    #d['test_data'] = data.loc[(data['ds'] < start_datetime)
    #                          & (data['ds'] <= max_datetime)]
    d['train_data'] = train_data

    return d

###########################

def create_model_m(d):
    """Create Prophet model using each input grid parameter set."""
    
    m = Prophet(seasonality_mode="multiplicative")
    d['model'] = m

    return d

###########################

def train_model_m(d):
    """Fit the model using the training data."""
    model = d['model']
    train_data = d['train_data']
    model.fit(train_data)
    d['model'] = model

    return d

###########################

def make_forecast_m(d):
    """Execute the forecast method on the model to make future predictions."""
    model = d['model']
    future = model.make_future_dataframe(periods=120, freq='H')
    
    forecast = model.predict(future)
    d['forecast'] = forecast

    return d

#########################

def reduce_data_scope_m(d):
    import pandas
    """Return a tuple (app + , + metric_type, {})."""
    return (
        d['symbol'],
        {
            'forecast': pandas.concat([d['train_data']['y'],d['forecast']], axis=1),  
        },
    )

##########################

def expand_predictions_m(d):
    import pytz, math
    
    symbol, data = d
    return [
        (
            symbol,
            pytz.utc.localize(p['ds']).to_pydatetime(),
            p['y'] if not(math.isnan(p['y'])) else None,
            p['yhat'],
            p['yhat_lower'],
            p['yhat_upper'],
        ) for i, p in data['forecast'].iterrows()
    ]

##########################

def main():
    
    spark = SparkSession.builder \
                        .appName('spark-bigquery-prophet')\
                        .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar') \
                        .getOrCreate()

    # Use the Cloud Storage bucket for temporary BigQuery export data used by the connector.
    bucket = 'temp_crypto_batch_' + google.auth.default()[1].replace('-','_') 
    spark.conf.set('temporaryGcsBucket', bucket)
    spark.conf.set("viewsEnabled","true")
    spark.conf.set("materializationDataset","temp_crypto_batch") 

    sql = """
            SELECT
              *
            FROM
            `temp_crypto_batch.batch_hour_interpo`
            WHERE
            tumble <= (
            SELECT
            TIMESTAMP_SUB(MAX(tumble), INTERVAL 5 DAY)
            FROM
            `temp_crypto_batch.batch_hour_interpo`)
          """

    df = spark.read.format("bigquery").load(sql)
    #df.show()
    
    df_prophet = df.select(df['symbol'], df['tumble'].alias('ds'), df['intrp'].alias('y')) \
               .groupBy('symbol')\
               .agg(collect_list(struct('ds', 'y')).alias('data')) \
               .rdd.map(lambda r: transform_data_m(r))\
                   .map(lambda d: partition_data_m(d))\
                   .filter(lambda d: len(d['train_data']) > 2)\
                   .map(lambda d: create_model_m(d))\
                   .map(lambda d: train_model_m(d))\
                   .map(lambda d: make_forecast_m(d))\
                   .map(lambda d: reduce_data_scope_m(d))\
                   .flatMap(lambda d: expand_predictions_m(d))

    #print(df_prophet.take(1))
    
    schema_for_m = StructType([
        StructField("symbol", StringType(), True),
        StructField("ds", TimestampType(), True),
        StructField("y", FloatType(), True),
        StructField("yhat", FloatType(), True),
        StructField("yhat_lower", FloatType(), True),
        StructField("yhat_upper", FloatType(), True),
    ])

    df_for_m = spark.createDataFrame(df_prophet, schema_for_m)

    df_for_m.write \
            .format("bigquery") \
            .option("temporaryGcsBucket", bucket) \
            .mode("overwrite") \
            .save("temp_crypto_batch.test_spark_global") 
    
    print("Insertion terminée")
    
if __name__ == "__main__":
    main()
