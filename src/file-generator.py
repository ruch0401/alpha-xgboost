import os
import sys
import pandas as pd

# source_file_path = os.path.join("file://", desktop_path, "output.csv")
source_file_path = "file:///home/ruchit/Desktop/output.csv"
resources_root = '../resources'


class CSVGeneratorParams:
    def __init__(self, is_internal, is_external, object_name, total_files, records_per_file, inserts, updates):
        self.is_internal = is_internal
        self.is_external = is_external
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
    print('Generating CSV file with internal_id: ', internal_id, ' and external_id: ', external_id)

    print('Reading source file')
    source_csv = pd.read_csv(source_file_path)
    print(f'Source file has {len(source_csv)} records')

    internal_id_col_name = params.object_name + "_id"
    external_id_col_name = params.object_name + "_external_id"
    external_ids = [f'ext_{i}' for i in range(1, 10000000 + 1)]
    max_pool_size_required = params.total_files * params.records_per_file

    if internal_id:
        pass

    if external_id:
        source_csv[external_id_col_name] = [f'ext_{i}' for i in range(1, 10000000 + 1)]
        source_csv = source_csv.head(max_pool_size_required)
        print(f'Selected {len(source_csv)} records from the source file')
        handle_first_file_write(source_csv, external_id_col_name, external_ids, params)
        for i in range(2, params.total_files + 2):
            start_index_for_inserts = params.inserts * (i - 1) + params.updates + 1
            end_index_for_inserts = start_index_for_inserts + params.inserts - 1

            start_index_for_updates = start_index_for_inserts - params.inserts
            end_index_for_updates = start_index_for_updates + params.updates - 1

            print(f'Inserts: ({start_index_for_inserts}, {end_index_for_inserts}) for iteration {i - 1}')
            print(f'Updates: ({start_index_for_updates}, {end_index_for_updates}) for iteration {i - 1}')

            insert_ids = [f'ext_{i}' for i in range(start_index_for_inserts, end_index_for_inserts + 1)]
            update_ids = [f'ext_{i}' for i in range(start_index_for_updates, end_index_for_updates + 1)]
            required_rows_inserts = source_csv[source_csv[external_id_col_name].isin(insert_ids)]
            required_rows_updates = source_csv[source_csv[external_id_col_name].isin(update_ids)]
            final_df = pd.concat([required_rows_inserts, required_rows_updates], axis=0)
            write_df_to_resources(final_df, f'{resources_root}/{params.object_name}_{i - 1}.csv')


def handle_first_file_write(df, external_id_col_name, external_ids, params):
    # first file is a special case where we need to insert all the records i.e. inserts + updates
    print('Handling first file write')
    ids = external_ids[:(params.inserts + params.updates)]
    df = df[df[external_id_col_name].isin(ids)]
    print(f'Selected {len(df)} records for handling first file write')
    write_df_to_resources(df, f'{resources_root}/{params.object_name}_base.csv')


if __name__ == '__main__':
    print('System arguments passed to the code are: ', sys.argv)

    global object_name, records_per_file, total_files, inserts, updates
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

    global internal_id, external_id
    if 'is_standard_object' in sys.argv:
        internal_id = True
        external_id = False
        generate_csv(
            CSVGeneratorParams(internal_id, external_id, object_name, total_files, records_per_file, inserts, updates))

    if 'is_standard_with_external_id' in sys.argv:
        internal_id = False
        external_id = True
        generate_csv(
            CSVGeneratorParams(internal_id, external_id, object_name, total_files, records_per_file, inserts, updates))

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
