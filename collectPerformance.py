#!/usr/bin/python

import json
import sys
from operator import itemgetter
from os import listdir
from os.path import isfile, join


def collectStatistics(analyzeApp):
    folder = '/tmp/spark-events/'

    captions = ["Load Graph", "Parse Graph", "Partition Graph", "Partition Graph", "Schema Computation",
                "Schema Computation", "Updates",
                "Load Graph", "Parse Graph", "Partition Graph", "Partition Graph", "Schema Computation",
                "Schema Computation", "Updates"]
    captionsMin = ["Load Graph", "Parse Graph", "Partition Graph", "Schema Computation", "Updates"]

    onlyfiles = [f for f in listdir(folder) if isfile(join(folder, f))]

    stats = []
    for fileName in onlyfiles:
        with open(folder + fileName) as f:
            iteration = {}
            content = f.readlines()
            batchVersion = -1
            for line in content:
                event = json.loads(line)

                if event['Event'] == 'SparkListenerEnvironmentUpdate':
                    appName = event['Spark Properties']['spark.app.name']
                    if appName.startswith(analyzeApp):
                        version = appName.replace(analyzeApp, '')
                        if version:
                            if '_batch_' in version:
                                batchVersion = int(version.replace('_batch_', ''))
                            else:
                                # if len([s for s in stats if 'iteration' in s and s['iteration'] == int(version)]) > 0:
                                found = False
                                for s in stats:
                                    if 'iteration' in s and s['iteration'] == int(version):
                                        iteration = s
                                        found = True
                                if not found:
                                    iteration['iteration'] = int(version)

                if event['Event'] == 'SparkListenerStageCompleted':
                    start = event['Stage Info']['Submission Time']
                    duration = event['Stage Info']['Completion Time'] - start
                    caption = captions[event['Stage Info']['Stage ID']]
                    if batchVersion >= 0:
                        update = False
                        for s in stats:
                            if 'iteration' in s and s['iteration'] == batchVersion:
                                print("Update batch: " + str(batchVersion) + ', adding ' + str(duration))

                                if 'batch' in s:
                                    s['batch'] = s['batch'] + duration
                                else:
                                    s['batch'] = duration
                                update = True
                        if not update:
                            print("Create batch: " + str(batchVersion))
                            iteration = {'iteration': batchVersion, 'batch': duration}
                            stats.append(iteration)

                        # print(iteration)
                    else:
                        if caption in iteration:
                            iteration[caption] = iteration[caption] + duration
                        else:
                            iteration[caption] = duration
                    # ['Stage Info']['Stage ID']
            if 'iteration' in iteration:
                if iteration not in stats:
                    stats.append(iteration)

    sortedStats = sorted(stats, key=itemgetter('iteration'))

    out = open('experiments/' + analyzeApp + '/' + analyzeApp + '-performance.csv', 'w')
    header = 'Iteration'
    for c in captionsMin:
        header = header + ',' + c
    out.write(header + ',Total,Batch\n')
    for s in sortedStats:
        row = str(s['iteration'])
        total = 0
        for c in captionsMin:
            row = row + ',' + str(s[c])
            total = total + s[c]
        batchDuration = ''
        if 'batch' in s:
            batchDuration = str(s['batch'])
        out.write(row + ',' + str(total) + ',' + batchDuration + '\n')
    print("Collected statistics for " + analyzeApp)


print(sys.argv)
sys.argv.pop(0)
print(sys.argv)
if len(sys.argv) > 0:
    for appName in sys.argv:
        collectStatistics(appName)
else:
    print("Missing app name.")
