import matplotlib.pyplot as plt
import numpy as np


def visualize_as_grouped_bar(y_data1, y_data2, x_data, y_data1_label, y_data2_label, x_axis_label, y_axis_label, plot_title, save_path):
    # Create an array of indices for the x-axis (position of bars)
    x = np.arange(len(x_data))

    # Width of the bars
    bar_width = 0.35

    plt.figure(figsize=(10, 6))
    bars1 = plt.bar(x - bar_width / 2, y_data1, bar_width, label=y_data1_label)
    bars2 = plt.bar(x + bar_width / 2, y_data2, bar_width, label=y_data2_label)
    plt.xlabel(x_axis_label)
    plt.ylabel(y_axis_label)
    plt.title(plot_title)
    plt.xticks(x, x_data)

    # Add data labels above the bars
    for bar1, bar2 in zip(bars1, bars2):
        height1 = bar1.get_height()
        height2 = bar2.get_height()
        plt.text(bar1.get_x() + bar1.get_width() / 2, height1 - 1, f'{height1:.2f}', ha='center', va='bottom')
        plt.text(bar2.get_x() + bar2.get_width() / 2, height2 - 1, f'{height2:.2f}', ha='center', va='bottom')

    plt.tight_layout()
    plt.legend()
    plt.show()
    plt.savefig(f'../output/my_plot_{save_path}.png')
