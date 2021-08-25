# MIT License
#
# Copyright (c) 2019 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import logging

from google.cloud import bigquery

from bigquery_to_pubsub.service.bigquery_to_file_job import BigQueryToFileJob
from bigquery_to_pubsub.service.sort_jsonlines_file_job import SortJsonLinesFileJob
from bigquery_to_pubsub.utils.file_utils import delete_file
from bigquery_to_pubsub.utils.random_utils import random_string


class TimeSeriesBigQueryToFileJob:

    def __init__(
            self,
            bigquery_table,
            start_timestamp,
            end_timestamp,
            timestamp_field,
            temp_bigquery_dataset,
            temp_bucket):
        self.bigquery_table = bigquery_table
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.timestamp_field = timestamp_field

        self.temp_bigquery_dataset = temp_bigquery_dataset
        self.temp_bucket = temp_bucket

        self.bigquery_client = bigquery.Client()

    def run(self):
        logging.info(
            'Started TimeSeriesBigQueryToFileJob with BigQuery table {}, start timestamp {}, end timestamp {}'.format(
                self.bigquery_table, self.start_timestamp, self.end_timestamp
            ))
       
        sql = (
            ' \
            WITH parsed_aggregator_oracle_requests AS ( \
            SELECT ARRAY( \
            SELECT JSON_EXTRACT_SCALAR(symbol_as_json, "$") \
            FROM UNNEST(JSON_EXTRACT_ARRAY(decoded_result.calldata, "$.symbols")) AS symbol_as_json \
            ) AS symbols, \
            CAST(JSON_EXTRACT_SCALAR(decoded_result.calldata, "$.multiplier") AS FLOAT64) AS multiplier, \
            ARRAY( \
                SELECT CAST(JSON_EXTRACT_SCALAR(rate_as_json, "$") AS FLOAT64) \
                FROM UNNEST(JSON_EXTRACT_ARRAY(decoded_result.result, "$.rates")) AS rate_as_json \
            ) AS rates, \
            block_timestamp, \
            block_timestamp_truncated, \
            oracle_request_id, \
            FROM `{table}` \
            WHERE request.oracle_script_id = 3 \
            AND decoded_result.calldata IS NOT NULL \
            AND decoded_result.result IS NOT NULL \
            ), \
            zipped_rates AS ( \
                SELECT block_timestamp, \
                block_timestamp_truncated, \
                oracle_request_id, \
                struct(symbol, rates[OFFSET(off)] AS rate) AS zipped, \
                multiplier, \
            FROM parsed_aggregator_oracle_requests, \
                UNNEST(symbols) AS symbol WITH OFFSET off \
                WHERE ARRAY_LENGTH(symbols) = ARRAY_LENGTH(rates) \
            ), \
            adjusted_rates AS ( \
                SELECT \
                    block_timestamp, \
                    block_timestamp_truncated, \
                    oracle_request_id, \
                    struct(zipped.symbol, CAST(IEEE_DIVIDE(zipped.rate, multiplier) AS STRING) AS rate) AS zipped, \
                    FROM zipped_rates \
            ) \
            SELECT \
                block_timestamp, \
                block_timestamp_truncated, \
                oracle_request_id, \
                zipped.symbol, \
                zipped.rate, \
                FROM adjusted_rates \
                WHERE `{timestamp_field}` >= "{start_timestamp}" AND `{timestamp_field}` < "{end_timestamp}" \
            '
        ).format(table=self.bigquery_table, timestamp_field=self.timestamp_field,
                 start_timestamp=self.start_timestamp, end_timestamp=self.end_timestamp) 

        '''
        sql = (
            "SELECT * FROM `{table}` "
            'WHERE {timestamp_field} >= "{start_timestamp}" and {timestamp_field} < "{end_timestamp}" '
        ).format(table=self.bigquery_table, timestamp_field=self.timestamp_field,
                 start_timestamp=self.start_timestamp, end_timestamp=self.end_timestamp)
        '''

        random_name = random_string(10)
        output_filename = random_name + '.json'

        bigquery_to_file_job = BigQueryToFileJob(sql, output_filename, self.temp_bigquery_dataset, self.temp_bucket)
        bigquery_to_file_job.run()

        sort_job = SortJsonLinesFileJob(filename=output_filename, sort_field=self.timestamp_field)
        sorted_filename = sort_job.run()

        delete_file(output_filename)

        return sorted_filename
