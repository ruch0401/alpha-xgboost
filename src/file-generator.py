import configparser
import os
import sys
from enum import Enum

import pandas as pd

source_file_path = "file:///home/ruchit/Desktop/output.csv"
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

    print('Reading source file')
    df = pd.read_csv(source_file_path)
    print(f'Source file has {len(df)} records')

    internal_id_col_name = params.object_name + "_id"
    external_id_col_name = params.object_name + "_external_id"
    external_ids = [f'ext_{i}' for i in range(1, 10000000 + 1)]
    max_pool_size_required = params.total_files * params.records_per_file
    df[external_id_col_name] = external_ids
    df = df.head(max_pool_size_required)
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
                required_rows_updates['AMOUNT'] = required_rows_updates['AMOUNT'].apply(lambda x: x + 1)

            global final_df
            if params.inserts == 0:
                final_df = required_rows_updates
            elif params.updates == 0:
                final_df = required_rows_inserts
            else:
                final_df = pd.concat([required_rows_inserts, required_rows_updates], axis=0)

            write_df_to_resources(final_df, f'{output_root}/{params.object_name}_{params.records_per_file}_{i}.csv')


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
    write_df_to_resources(df, f'{output_root}/{params.object_name}_0.csv')


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
