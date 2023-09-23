import os
import sys

import pandas as pd

# source_file_path = os.path.join("file://", desktop_path, "output.csv")
source_file_path = "file:///home/ruchit/Desktop/output.csv"
resources_root = '../output'

# Suppress SettingWithCopyWarning
pd.options.mode.chained_assignment = None  # 'None' means no warnings will be shown


class CSVGeneratorParams:
    def __init__(self, object_name, total_files, records_per_file, inserts, updates, part_of_candidate_key=None):
        self.part_of_candidate_key = part_of_candidate_key
        self.object_name = object_name
        self.total_files = total_files
        self.records_per_file = records_per_file
        self.inserts = inserts
        self.updates = updates


def write_df_to_resources(final_df, path):
    if os.path.exists(path):
        os.remove(path)
    final_df.to_csv(path, index=False)


def generate_csv(params: CSVGeneratorParams):
    print('Starting CSV file generation')

    print('Reading source file')
    source_csv = pd.read_csv(source_file_path)
    print(f'Source file has {len(source_csv)} records')

    internal_id_col_name = params.object_name + "_id"
    external_id_col_name = params.object_name + "_external_id"
    external_ids = [f'ext_{i}' for i in range(1, 10000000 + 1)]
    max_pool_size_required = params.total_files * params.records_per_file
    source_csv[external_id_col_name] = external_ids
    source_csv = source_csv.head(max_pool_size_required)
    print(f'Selected {len(source_csv)} records from the source file')
    handle_first_file_write(source_csv, external_ids, params)
    for i in range(0, params.total_files):
        start_index_for_inserts = params.inserts * (i + 1) + params.updates + 1
        end_index_for_inserts = start_index_for_inserts + params.inserts - 1

        start_index_for_updates = start_index_for_inserts - params.inserts
        end_index_for_updates = start_index_for_updates + params.updates - 1

        print(f'Inserts: ({start_index_for_inserts}, {end_index_for_inserts}) for iteration {i + 1}')
        print(f'Updates: ({start_index_for_updates}, {end_index_for_updates}) for iteration {i + 1}')

        insert_ids = [f'ext_{i}' for i in range(start_index_for_inserts, end_index_for_inserts + 1)]
        update_ids = [f'ext_{i}' for i in range(start_index_for_updates, end_index_for_updates + 1)]
        required_rows_inserts = source_csv[source_csv[external_id_col_name].isin(insert_ids)]
        required_rows_updates = source_csv[source_csv[external_id_col_name].isin(update_ids)]

        if is_internal_id_required():
            required_rows_inserts.rename(columns={external_id_col_name: internal_id_col_name}, inplace=True)
            required_rows_inserts[internal_id_col_name] = required_rows_inserts[internal_id_col_name].apply(
                lambda x: x.replace(x, ''))
            required_rows_updates.rename(columns={external_id_col_name: internal_id_col_name}, inplace=True)
            required_rows_updates[internal_id_col_name] = required_rows_updates[internal_id_col_name].str.split('_').str[1]

        if is_external_id_required():
            print('Default code path which handles the external_id case is being executed')

        if is_conditional_key_required():
            required_rows_inserts = required_rows_inserts.remove(columns=[external_id_col_name])
            required_rows_updates = required_rows_updates.remove(columns=[external_id_col_name])
            # all the candidate keys will have to be changed to simulate inserts
            candidate_keys = params.part_of_candidate_key.split(',')
            for key in candidate_keys:
                required_rows_inserts[key] = required_rows_inserts[key].apply(lambda x: x + str(i + 1))

            # one of the other columns, lets say the 'amount' column will have to be changed to simulate updates, keeping the candidate keys same
            required_rows_updates['amount'] = required_rows_updates['amount'].apply(lambda x: int(x) + 1)

        final_df = pd.concat([required_rows_inserts, required_rows_updates], axis=0)
        write_df_to_resources(final_df, f'{resources_root}/{params.object_name}_{i + 1}.csv')


def is_internal_id_required():
    return 'is_standard' in sys.argv


def is_external_id_required():
    return ('is_standard_with_external_id' in sys.argv
            or 'is_standard_bitemporal' in sys.argv
            or 'is_versioned_bitemporal' in sys.argv)


def is_conditional_key_required():
    return ('is_standard_transactional' in sys.argv
            or 'is_versioned_transactional' in sys.argv
            or 'is_complete_snapshot' in sys.argv
            or 'is_incremental_snapshot' in sys.argv)


def handle_first_file_write(df, external_ids, params):
    internal_id_col_name = params.object_name + "_id"
    external_id_col_name = params.object_name + "_external_id"
    ids = external_ids[:(params.inserts + params.updates)]
    df = df[df[external_id_col_name].isin(
        ids)]  # default case is all object types which require external_id (standard_with_external_id, standard_bitemporal, versioned_bitemporal)

    if is_internal_id_required():
        df.rename(columns={external_id_col_name: internal_id_col_name}, inplace=True)
        df[internal_id_col_name] = df[internal_id_col_name].apply(lambda x: x.replace(x, ''))  # since internal_id is auto-generated, we need to remove the values from the first file

    if is_external_id_required():
        print('Default code path which handles the external_id case is being executed')

    if is_conditional_key_required():
        df = df.remove(columns=[external_id_col_name])

    write_df_to_resources(df, f'{resources_root}/{params.object_name}_base.csv')


def parse_command_line():
    global object_name, records_per_file, total_files, inserts, updates, part_of_candidate_key
    if 'object_name' not in sys.argv:
        print('Object name is required in the system arguments')
        sys.exit(1)
    else:
        object_name = sys.argv[sys.argv.index('object_name') + 1]

    if 'total_files' not in sys.argv:
        print('Total files is required in the system arguments')
        sys.exit(1)
    else:
        total_files = int(sys.argv[sys.argv.index('total_files') + 1])

    if 'records_per_file' not in sys.argv:
        print('Records per file is required in the system arguments')
        sys.exit(1)
    else:
        records_per_file = int(sys.argv[sys.argv.index('records_per_file') + 1])

    if 'inserts' not in sys.argv:
        print('Inserts is required in the system arguments')
        sys.exit(1)
    else:
        inserts = int(sys.argv[sys.argv.index('inserts') + 1])

    if 'updates' not in sys.argv:
        print('Updates is required in the system arguments')
        sys.exit(1)
    else:
        updates = int(sys.argv[sys.argv.index('updates') + 1])

    if is_conditional_key_required():
        if 'part_of_candidate_key' not in sys.argv:
            print('part_of_candidate_key is required in the system arguments when the object type is set to transactional or snapshot')
            sys.exit(1)
        part_of_candidate_key = sys.argv[sys.argv.index('part_of_candidate_key') + 1]


if __name__ == '__main__':
    print('System arguments passed to the code are: ', sys.argv)

    object_name = None
    total_files = None
    records_per_file = None
    inserts = None
    updates = None
    part_of_candidate_key = None

    parse_command_line()
    generate_csv(
        CSVGeneratorParams(object_name, total_files, records_per_file, inserts, updates, part_of_candidate_key))

    # total_files = 4
    # inserts = 70
    # updates = 30
    # for i in range(2, total_files + 2):
    #     start_index_for_inserts = inserts * (i - 1) + updates + 1
    #     end_index_for_inserts = start_index_for_inserts + inserts - 1
    #
    #     start_index_for_updates = start_index_for_inserts - inserts
    #     end_index_for_updates = start_index_for_updates + updates - 1
    #
    #     print(f'Inserts: ({start_index_for_inserts}, {end_index_for_inserts}) for iteration {i - 1}')
    #     print(f'Updates: ({start_index_for_updates}, {end_index_for_updates}) for iteration {i - 1}')
