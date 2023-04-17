import pandas as pd
import plotly.express as px


def visualize_as_grouped_bar(data):
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
