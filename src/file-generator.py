import configparser
import os
import sys
from enum import Enum
import random

import pandas as pd

output_root = '../output'

# Suppress SettingWithCopyWarning
pd.options.mode.chained_assignment = None  # 'None' means no warnings will be shown
config = configparser.ConfigParser()
config.read('../resources/config.ini')

global object_type, object_name, records_per_file, total_files, inserts, updates, part_of_candidate_key


class CSVGeneratorParams:
    def __init__(self, object_name, total_files, records_per_file, inserts, updates, part_of_candidate_key=None):
        self.part_of_candidate_key = part_of_candidate_key
        self.object_name = object_name
        self.total_files = total_files
        self.records_per_file = records_per_file
        self.inserts = inserts
        self.updates = updates


class ObjectType(Enum):
    STANDARD = 'standard'
    STANDARD_WITH_EXTERNAL_ID = 'standard_with_external_id'
    STANDARD_BITEMPORAL = 'standard_bitemporal'
    VERSIONED_BITEMPORAL = 'versioned_bitemporal'
    STANDARD_TRANSACTIONAL = 'standard_transactional'
    VERSIONED_TRANSACTIONAL = 'versioned_transactional'
    COMPLETE_SNAPSHOT = 'complete_snapshot'
    INCREMENTAL_SNAPSHOT = 'incremental_snapshot'


def write_df_to_resources(final_df, path):
    if os.path.exists(path):
        os.remove(path)
    final_df.to_csv(path, index=False)


def generate_csv(params: CSVGeneratorParams):
    print('Starting CSV file generation')
    max_pool_size_required = params.total_files * params.records_per_file

    print('Generating source dataframe')
    # df = pd.read_csv(source_file_path)
    df = generate_source_dataframe(max_pool_size_required)
    print(f'Source file has {len(df)} records')

    internal_id_col_name = params.object_name + "_id"
    external_id_col_name = params.object_name + "_external_id"
    external_ids = [f'ext_{i}' for i in range(1, max_pool_size_required + 1)]

    df[external_id_col_name] = external_ids
    df = df.head(max_pool_size_required)

    if len(df) < max_pool_size_required:
        print(f'An error occurred while generating the source dataframe. Source dataframe should have had same number of records as the max pool size required')
        sys.exit(1)

    print(f'Selected {len(df)} records from the source file')
    global start_index_for_inserts, end_index_for_inserts, start_index_for_updates, end_index_for_updates
    for i in range(0, params.total_files):
        if i == 0:
            start_index_for_inserts = 1
            end_index_for_inserts = params.records_per_file
            print(f'Inserts: ({start_index_for_inserts}, {end_index_for_inserts}) for iteration {i + 1}')
            print(f'Skipping updates since first iteration will have only inserts')
            generate_file_with_seed_data(df, external_ids, params)
        else:
            start_index_for_inserts = end_index_for_inserts + 1
            end_index_for_inserts = start_index_for_inserts + params.inserts - 1
            end_index_for_updates = start_index_for_inserts - 1
            start_index_for_updates = end_index_for_updates - params.updates + 1
            print(f'Inserts: ({start_index_for_inserts}, {end_index_for_inserts}) for iteration {i + 1}')
            print(f'Updates: ({start_index_for_updates}, {end_index_for_updates}) for iteration {i + 1}')

            insert_ids = [f'ext_{i}' for i in range(start_index_for_inserts, end_index_for_inserts + 1)]
            update_ids = [f'ext_{i}' for i in range(start_index_for_updates, end_index_for_updates + 1)]
            required_rows_inserts = df[df[external_id_col_name].isin(insert_ids)]
            required_rows_updates = df[df[external_id_col_name].isin(update_ids)]

            if is_internal_id_required():
                required_rows_inserts.rename(columns={external_id_col_name: internal_id_col_name}, inplace=True)
                required_rows_inserts[internal_id_col_name] = required_rows_inserts[internal_id_col_name].apply(
                    lambda x: x.replace(x, ''))
                required_rows_updates.rename(columns={external_id_col_name: internal_id_col_name}, inplace=True)
                required_rows_updates[internal_id_col_name] = \
                    required_rows_updates[internal_id_col_name].str.split('_').str[1]

            if is_external_id_required():
                print('Default code path which handles the external_id case is being executed')

            if is_conditional_key_required():
                required_rows_inserts = required_rows_inserts.drop(external_id_col_name, axis=1)
                required_rows_updates = required_rows_updates.drop(external_id_col_name, axis=1)
                # all the candidate keys will have to be changed to simulate inserts
                candidate_keys = params.part_of_candidate_key.split(',')
                for key in candidate_keys:
                    required_rows_inserts[key] = required_rows_inserts[key].apply(lambda x: x + 1)

                # one of the other columns, lets say the 'amount' column will have to be changed to simulate updates, keeping the candidate keys same
                required_rows_updates['TOTAL_AMOUNT'] = required_rows_updates['TOTAL_AMOUNT'].apply(lambda x: x + 1)

            global final_df
            if params.inserts == 0:
                final_df = required_rows_updates
            elif params.updates == 0:
                final_df = required_rows_inserts
            else:
                final_df = pd.concat([required_rows_inserts, required_rows_updates], axis=0)

            suffix = get_suffix(i)
            write_df_to_resources(final_df, f'{output_root}/{params.object_name}_{params.records_per_file}_{suffix}.csv')


def get_suffix(i):
    if i < 10:
        suffix = f'00{i}'
    elif 10 <= i < 100:
        suffix = f'0{i}'
    else:
        suffix = f'{i}'
    return suffix


def generate_file_with_seed_data(df, external_ids, params):
    internal_id_col_name = params.object_name + "_id"
    external_id_col_name = params.object_name + "_external_id"
    df = df[df[external_id_col_name].isin(external_ids[:params.records_per_file])]
    if is_internal_id_required():
        df.rename(columns={external_id_col_name: internal_id_col_name}, inplace=True)
        # since internal_id is auto-generated, we need to remove the values from the first file
        df[internal_id_col_name] = df[internal_id_col_name].apply(lambda x: x.replace(x, ''))
    if is_external_id_required():
        print('Default code path which handles the external_id case is being executed')

    if is_conditional_key_required():
        df = df.drop(external_id_col_name, axis=1)
    write_df_to_resources(df, f'{output_root}/{params.object_name}_{params.records_per_file}_000.csv')


def is_internal_id_required():
    return object_type == ObjectType.STANDARD.value


def is_external_id_required():
    return (object_type == ObjectType.STANDARD_WITH_EXTERNAL_ID.value
            or object_type == ObjectType.STANDARD_BITEMPORAL.value
            or object_type == ObjectType.VERSIONED_BITEMPORAL.value)


def is_conditional_key_required():
    return (object_type == ObjectType.STANDARD_TRANSACTIONAL.value
            or object_type == ObjectType.VERSIONED_TRANSACTIONAL.value
            or object_type == ObjectType.COMPLETE_SNAPSHOT.value
            or object_type == ObjectType.INCREMENTAL_SNAPSHOT.value)


def get_random_number(start, end):
    return random.randint(start, end)


def init_and_parse_config():
    global object_type, object_name, total_files, records_per_file, inserts, updates, part_of_candidate_key
    object_type = config['default'].get('object_type', '')
    object_name = config['default'].get('object_name', '')
    total_files = int(config['default'].get('total_files', '-1'))
    records_per_file = int(config['default'].get('records_per_file', '-1'))
    inserts = int(config['default'].get('inserts', '-1'))
    updates = int(config['default'].get('updates', '-1'))
    part_of_candidate_key = config['default'].get('part_of_candidate_key', '')

    if object_type == '' or object_name == '' or total_files == -1 or records_per_file == -1 or inserts == -1 or updates == -1:
        print('All the parameters are required in the config.ini file')
        sys.exit(1)

    if inserts + updates != records_per_file:
        print('Sum of inserts and updates should be equal to records_per_file')
        sys.exit(1)

    if is_conditional_key_required():
        if part_of_candidate_key == '':
            print(
                'part_of_candidate_key is required in the system arguments when the object type is set to transactional or snapshot')
            sys.exit(1)


def generate_source_dataframe(records):
    data = {
        "PRODUCT_ID": [get_random_number(1, 7) for i in range(0, records)],
        "CHANNEL_ID": [get_random_number(1, 7) for i in range(0, records)],
        "MARKET_ID": [i for i in range(0, records)],
        "CUSTOMER_ID": [get_random_number(100, 100000) for i in range(0, records)],
        "NRX": [get_random_number(100, 1000) for i in range(0, records)],
        "TRX": [get_random_number(100, 1000) for i in range(0, records)],
        "FACTORED_NRX": [get_random_number(100, 1000) for i in range(0, records)],
        "FACTORED_TRX": [get_random_number(100, 1000) for i in range(0, records)],
        "TOTAL_UNITS": [get_random_number(100, 1000) for i in range(0, records)],
        "TOTAL_AMOUNT": [get_random_number(100, 1000) for i in range(0, records)],
        "CUSTOM_METRIC1": [get_random_number(100, 1000) for i in range(0, records)],
        "CUSTOM_METRIC2": [get_random_number(100, 1000) for i in range(0, records)],
        "CUSTOM_METRIC3": [get_random_number(100, 1000) for i in range(0, records)],
        "CUSTOM_METRIC4": [get_random_number(100, 1000) for i in range(0, records)],
        "CUSTOM_METRIC5": [get_random_number(100, 1000) for i in range(0, records)],
        "CUSTOM_METRIC6": [get_random_number(100, 1000) for i in range(0, records)],
        "CUSTOM_METRIC7": [get_random_number(100, 1000) for i in range(0, records)],
        "CUSTOM_METRIC8": [get_random_number(100, 1000) for i in range(0, records)],
        "CUSTOM_METRIC9": [get_random_number(100, 1000) for i in range(0, records)],
        "CUSTOM_METRIC10": [get_random_number(100, 1000) for i in range(0, records)],
        "IS_DELETED": ['FALSE' for i in range(0, records)],
    }
    df = pd.DataFrame(data)
    print(df.head())
    return df


if __name__ == '__main__':
    init_and_parse_config()
    generate_csv(
        CSVGeneratorParams(object_name, total_files, records_per_file, inserts, updates, part_of_candidate_key))

    # total_files = 5
    # inserts = 0
    # updates = 100
    # records_per_file = 100
    # global start_index_for_inserts, end_index_for_inserts, start_index_for_updates, end_index_for_updates
    # for i in range(0, total_files):
    #     if i == 0:
    #         start_index_for_inserts = 1
    #         end_index_for_inserts = records_per_file
    #         print(f'Inserts: ({start_index_for_inserts}, {end_index_for_inserts}) for iteration {i + 1}')
    #         print(f'Skipping updates since first iteration will have only inserts')
    #     else:
    #         start_index_for_inserts = end_index_for_inserts + 1
    #         end_index_for_inserts = start_index_for_inserts + inserts - 1
    #         end_index_for_updates = start_index_for_inserts - 1
    #         start_index_for_updates = end_index_for_updates - updates + 1
    #         print(f'Inserts: ({start_index_for_inserts}, {end_index_for_inserts}) for iteration {i + 1}')
    #         print(f'Updates: ({start_index_for_updates}, {end_index_for_updates}) for iteration {i + 1}')
