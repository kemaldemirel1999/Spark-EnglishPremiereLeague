import glob
import os

import pandas as pd
from matplotlib import pyplot as plt
import numpy as np
import shutil



def scored_goals_plot(path):
    plt.rcParams["figure.figsize"] = [7.00, 3.50]
    plt.rcParams["figure.autolayout"] = True
    columns = ["full", "score", "creativity"]
    df = pd.read_csv(path, usecols=columns)
    df.creativity = np.round(df.creativity).astype(np.uint8)
    df.score = np.round(df.score).astype(np.uint8)
    df = df[0:20]
    x = np.arange(len(df))  # the label locations
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width/2, df.score, width, label='Goals')
    rects2 = ax.bar(x + width/2, df.creativity, width, label='Creativity')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Goals and Creativity scores')
    ax.set_title('Names')
    ax.set_xticks(x, df.full)
    ax.legend()

    ax.bar_label(rects1, padding=3)
    ax.bar_label(rects2, padding=3)

    fig.autofmt_xdate()

    plt.show()


def shots_on_target_plot(path):
    plt.rcParams["figure.figsize"] = [7.00, 3.50]
    plt.rcParams["figure.autolayout"] = True
    columns = ["teamId","TotalShotsWhenHome",
               "TotalShotsOnTargetWhenHome",
               "RatioShotsOnTargetPerMatchWhenHome",
               "RatioShotsOnTargetPerMatchWhenAway",
               "TotalShotsWhenAway",
               "TotalShotsOnTargetWhenAway"
]
    df = pd.read_csv(path, usecols=columns)
    df = df[0:20]
    x = np.arange(len(df))  # the label locations
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width / 2, df.RatioShotsOnTargetPerMatchWhenHome, width, label='Home')
    rects2 = ax.bar(x + width / 2, df.RatioShotsOnTargetPerMatchWhenAway, width, label='Away')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Goals and Creativity scores')
    ax.set_title('Teams')
    ax.set_xticks(x, df.teamId)
    ax.legend()

    fig.autofmt_xdate()

    plt.show()


if __name__ == '__main__':
    performanceTablePath = os.getcwd() + "/src/main/performanceTable"
    ratioTablePath = os.getcwd() + "/src/main/ratioTable"

    performanceTablePath = glob.glob(os.path.join(performanceTablePath, "*.csv"))
    ratioTablePath = glob.glob(os.path.join(ratioTablePath, "*.csv"))

    scored_goals_plot(performanceTablePath[0])
    shots_on_target_plot(ratioTablePath[0])
    shutil.rmtree(os.getcwd() + "/src/main/ratioTable")
    shutil.rmtree(os.getcwd() + "/src/main/performanceTable")