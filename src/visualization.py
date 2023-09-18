import pandas as pd
import plotly.express as px
import sys


def visualize_as_grouped_bar(data):
    print(f"Plotting graph for data: {data}")
    df_list = []
    for key, values in data.items():
        for entry in values:
            df_list.append({"Combination": str(key), "Type": "Read", "Time": entry["readTime"]})
            df_list.append({"Combination": str(key), "Type": "Write", "Time": entry["writeTime"]})

    df = pd.DataFrame(df_list)

    fig = px.bar(df, x="Combination", y="Time", color="Type", barmode="group", title="Read and Write Times")
    fig.show()
    fig.write_image(f'../output/graph_{get_suffix()}.png')
    fig.write_html(f'../output/graph_{get_suffix()}.html')


def log_to_file(data):
    with open(f'../output/analytics_{get_suffix()}.json', 'w') as f:
        f.write(str(data))


def get_suffix():
    suffix = ''
    if 'is_standard_object' in sys.argv:
        suffix = '_standard_object'
    if 'is_standard_with_external_id' in sys.argv:
        suffix = '_standard_with_external_id'
    if 'is_bitemporal_object' in sys.argv:
        suffix = '_bitemporal_object'
    if 'is_complete_snapshot_object' in sys.argv:
        suffix = '_complete_snapshot_object'
    if 'is_incremental_snapshot_object' in sys.argv:
        suffix = '_incremental_snapshot_object'
    if 'is_transactional_object' in sys.argv:
        suffix = '_transactional_object'
    return suffix
