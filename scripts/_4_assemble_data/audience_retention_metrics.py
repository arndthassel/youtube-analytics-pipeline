import pandas as pd
import numpy as np


def retention_after_60_sec(data, ar_data):
    print('getting audience retention after 1 minute')
    rel_elapsed_after_60_sec = (60 / data['length (s)']).round(decimals=2)
    metric_list = []

    for i in data['#']:
        video_set = ar_data[(ar_data['videoNumber'] == i)]
        if rel_elapsed_after_60_sec[i-1] <= 1:
            retention_value = video_set.query(
                f'elapsedVideoTimeRatio=={str(rel_elapsed_after_60_sec[i-1])}')['audienceWatchRatio'].iloc[0]
            metric_list.append(retention_value)
        else:
            metric_list.append(np.nan)

    data['retention after 60s'] = metric_list
    return (data['retention after 60s'])
