#!/usr/bin/python

import json
import sys
from operator import itemgetter
from os import listdir, makedirs
from os.path import isfile, join, isdir
import pandas as pd

def collectStatistics(analyze_app):
    folder = '/tmp/spark-events/'
    captions = ["Iteration",
                "Incremental Start",
                "Incremental End",
                "Incremental Duration",
                "Batch Start",
                "Batch End",
                "Batch Duration"]

    data_frame = pd.DataFrame(columns=captions)
    data_frame.set_index('Iteration')

    onlyfiles = [f for f in listdir(folder) if isfile(join(folder, f))]

    for fileName in onlyfiles:
        with open(folder + fileName) as f:
            content = f.readlines()
            batch_computation = False
            iteration_number = -1
            for line in content:
                event = json.loads(line)
                print(event)
                # if event['Event'] == 'SparkListenerEnvironmentUpdate':
                #     app_name = event['Spark Properties']['spark.app.name']
                #     if app_name.startswith(analyze_app):
                #         version = app_name.replace(analyze_app, '')
                #         if version:
                #             if '_batch_' in version:
                #                 batch_computation = True
                #                 iteration_number = int(version.replace('_batch_', ''))
                #                 if data_frame.loc[data_frame['Iteration'] == iteration_number].empty:
                #                     data_frame.loc[len(data_frame)] = iteration_number
                #
                #             else:
                #                 batch_computation = False
                #                 iteration_number = int(version)
                #                 if data_frame.loc[data_frame['Iteration'] == iteration_number].empty:
                #                     data_frame.loc[len(data_frame)] = iteration_number
                #
                #
                #
                # if event['Event'] == 'SparkListenerJobStart':
                #     start = event['Submission Time']
                #     caption = 'Incremental Start'
                #     if batch_computation:
                #         caption = 'Batch Start'
                #
                #     data_frame.loc[data_frame['Iteration'] == iteration_number, caption] = start
                #
                # if event['Event'] == 'SparkListenerJobEnd':
                #     end = event['Completion Time']
                #     caption = 'Incremental End'
                #     if batch_computation:
                #         caption = 'Batch End'
                #     data_frame.loc[data_frame['Iteration'] == iteration_number, caption] = end

    # data_frame['Incremental Duration'] = data_frame['Incremental End'] - data_frame['Incremental Start']
    # data_frame['Batch Duration'] = data_frame['Batch End'] - data_frame['Batch Start']
    # data_frame = data_frame.sort_values(['Iteration'], ascending=[True])
    # print(data_frame)
    # if not isdir('experiments/' + analyze_app):
    #     makedirs('experiments/' + analyze_app)
    # data_frame.to_csv('experiments/' + analyze_app + '/' + analyze_app + '-performance.csv', index=False)
    # print("Collected statistics for " + analyze_app)


print(sys.argv)
sys.argv.pop(0)
print(sys.argv)
if len(sys.argv) > 0:
    for appName in sys.argv:
        collectStatistics(appName)
else:
    print("Missing app name.")
