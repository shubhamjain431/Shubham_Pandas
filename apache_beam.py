
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Define the pipeline options
pipeline_options = PipelineOptions()

# Create a pipeline
with beam.Pipeline(options=pipeline_options) as pipeline:
    # Read dataset1 and dataset2
    dataset1 = (
        pipeline
        | 'Read Dataset1' >> beam.io.ReadFromText('dataset1.csv', skip_header_lines=1)
        | 'Parse Dataset1' >> beam.Map(lambda line: line.split(','))
        | 'Convert to Dict Dataset1' >> beam.Map(lambda fields: {
            'invoice_id': int(fields[0]),
            'legal_entity': fields[1],
            'counter_party': fields[2],
            'rating': int(fields[3]),
            'status': fields[4],
            'value': int(fields[5])
        })
    )

    dataset2 = (
        pipeline
        | 'Read Dataset2' >> beam.io.ReadFromText('dataset2.csv', skip_header_lines=1)
        | 'Parse Dataset2' >> beam.Map(lambda line: line.split(','))
        | 'Convert to Dict Dataset2' >> beam.Map(lambda fields: {
            'counter_party': fields[0],
            'tier': int(fields[1])
        })
    )

    # Perform the join
    merged_data = (
        {'dataset1': dataset1, 'dataset2': dataset2}
        | 'Merge Datasets' >> beam.CoGroupByKey()
        | 'Extract Merged Data' >> beam.Values()
        | 'Flatten Merged Data' >> beam.FlatMap(lambda dicts: [
            {
                'invoice_id': data['invoice_id'],
                'legal_entity': data['legal_entity'],
                'counter_party': data['counter_party'],
                'rating': data['rating'],
                'status': data['status'],
                'value': data['value'],
                'tier': tier['tier'][0] if tier['tier'] else None
            }
            for data in dicts['dataset1']
            for tier in dicts['dataset2']
        ])
    )

    # Calculate the maximum rating by counterparty
    max_rating = (
        merged_data
        | 'Map Counter Party and Rating' >> beam.Map(lambda data: (data['counter_party'], data['rating']))
        | 'Group By Counter Party' >> beam.GroupByKey()
        | 'Calculate Max Rating' >> beam.CombinePerKey(max)
        | 'Format Max Rating' >> beam.Map(lambda kv: {
            'counter_party': kv[0],
            'max_rating': kv[1]
        })
    )

    # Calculate the sum of values where status is ARAP and ACCR
    sum_arap = (
        merged_data
        | 'Filter ARAP' >> beam.Filter(lambda data: data['status'] == 'ARAP')
        | 'Map Counter Party and Value (ARAP)' >> beam.Map(lambda data: (data['counter_party'], data['value']))
        | 'Group By Counter Party (ARAP)' >> beam.GroupByKey()
        | 'Calculate Sum Value (ARAP)' >> beam.CombinePerKey(sum)
        | 'Format Sum Value (ARAP)' >> beam.Map(lambda kv: {
            'counter_party': kv[0],
            'sum_value_arap': kv[1]
        })
    )

    sum_accr = (
        merged_data
        | 'Filter ACCR' >> beam.Filter(lambda data: data['status'] == 'ACCR')
        | 'Map Counter Party and Value (ACCR)' >> beam.Map(lambda data: (data['counter_party'], data['value']))
        | 'Group By Counter Party (ACCR)' >> beam.GroupByKey()
        | 'Calculate Sum Value (ACCR)' >> beam.CombinePerKey(sum)
        | 'Format Sum Value (ACCR)' >> beam.Map(lambda kv: {
            'counter_party': kv[0],
            'sum_value_accr': kv[1]
        })
    )

    # Merge the max_rating, sum_arap, and sum_accr back into the merged_data
    merged_data = (
        {'merged_data': merged_data, 'max_rating': max_rating, 'sum_arap': sum_arap, 'sum_accr': sum_accr}
        | 'Merge Data' >> beam.CoGroupByKey()
        | 'Extract Merged Data (Updated)' >> beam.Values()
        | 'Flatten Merged Data (Updated)' >> beam.FlatMap(lambda dicts: [
            {
                'invoice_id': data['invoice_id'],
                'legal_entity': data['legal_entity'],
                'counter_party': data['counter_party'],
                'rating': data['rating'],
                'status': data['status'],
                'value': data['value'],
                'tier': data['tier'][0] if data['tier'] else None,
                'max_rating': data['max_rating'][0] if data['max_rating'] else None,
                'sum_value_arap': data['sum_arap'][0] if data['sum_arap'] else None,
                'sum_value_accr': data['sum_accr'][0] if data['sum_accr'] else None
            }
            for data in dicts['merged_data']
        ])
    )

    # Select the desired columns
    result = (
        merged_data
        | 'Select Columns' >> beam.Map(lambda data: (
            data['legal_entity'],
            data['counter_party'],
            data['tier'],
            data['max_rating'],
            data['sum_value_arap'],
            data['sum_value_accr']
        ))
    )

    # Drop duplicate rows based on counter_party
    result = (
        result
        | 'Add Key' >> beam.Map(lambda data: (data[1], data))
        | 'Group By Key' >> beam.GroupByKey()
        | 'Get First Value' >> beam.Map(lambda kv: kv[1][0])
    )

    # Calculate totals for each legal_entity, counterparty, and tier
    totals = (
        result
        | 'Calculate Totals' >> beam.CombineGlobally(lambda data: [
            {
                'legal_entity': 'Total',
                'counter_party': 'Total',
                'tier': 'Total',
                'max_rating': max(row['max_rating'] for row in data) if data else None,
                'sum_value_arap': sum(row['sum_value_arap'] for row in data) if data else None,
                'sum_value_accr': sum(row['sum_value_accr'] for row in data) if data else None
            }
        ])
        | 'Explode List' >> beam.FlatMap(lambda lst: lst)
    )

    # Write the output to a CSV file
    result_and_totals = (
        (result, 'apache_output.csv'),
        (totals, 'apache_totals.csv')
    )
    for pc in result_and_totals:
        pc[0] | f'Write to CSV ({pc[1]})' >> beam.io.WriteToText(pc[1], header='legal_entity,counter_party,tier,max_rating,sum_value_arap,sum_value_accr', num_shards=1, shard_name_template='')
