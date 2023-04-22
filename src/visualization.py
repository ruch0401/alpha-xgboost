import pandas as pd
import plotly.express as px
import json


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
    fig.write_image(f'../output/graph.png')
    fig.write_html(f'../output/graph.html')


def log_to_file(data):
    with open('../output/analytics.json', 'w') as f:
        f.write(str(data))


if __name__ == '__main__':
    data = {(1, 10): [{'readTime': 6.773444, 'writeTime': 20.550207}], (1, 20): [{'readTime': 7.221863, 'writeTime': 16.929979}], (2, 10): [{'readTime': 6.805896, 'writeTime': 16.926752}, {'readTime': 6.36243, 'writeTime': 16.348794}], (2, 20): [{'readTime': 7.012962, 'writeTime': 16.300162}, {'readTime': 7.07328, 'writeTime': 15.208348}]}
    with open('../output/analytics.json', 'w') as f:
        f.write(str(data))