import os
from os import listdir
from os.path import isfile, join
import re

#* --- paperTitle
#@ --- Authors
#year ---- Year
#conf --- publication venue
#citation --- citation number (both -1 and 0 means none)
#index ---- index id of this paper
#arnetid ---- pid in arnetminer database
#% ---- the id of references of this paper (there are multiple lines, with each indicating a reference)
#! --- Abstract


# For V5-V6
input = 'arnetminer-citation-network'
out_file = None

def parse_attribute(id, label, attribute):
    return "<"+id+"> <"+label+"> \""+re.sub("(<|>)", "", attribute)+"\" <"+input+"> .\n"

def parse_reference(id, label, reference):
    return "<"+id+"> <"+label+"> <"+reference+">  <"+input+">.\n"


stats = {}

def export_stats(paper):
    if"id" in paper:
        year = paper["year"]
        if year not in stats:
            stats[year] = set()
        stats[year].add(paper['id'])

def export_paper(paper):
    if "id" in paper:
        year = paper["year"]

        if "title" in paper:
            out_file.write(parse_attribute(paper["id"], "title", paper["title"]))
        if "authors" in paper:
            out_file.write(parse_attribute(paper["id"], "authors", paper["authors"]))
        if "year" in paper:
            out_file.write(parse_attribute(paper["id"], "year", paper["year"]))
        if "venue" in paper:
            out_file.write(parse_attribute(paper["id"], "venue", paper["venue"]))
        if "abstract" in paper:
            out_file.write(parse_attribute(paper["id"], "abstract", paper["abstract"]))
        if "references" in paper:
            for reference in paper["references"]:
                out_file.write(parse_reference(paper["id"], "reference", reference))

def parse_file(file, handle_paper=export_paper):
    with open(file) as file_in:
        tmp_paper = {}
        for line in file_in:
            if line.startswith("#*"):
                handle_paper(tmp_paper)
                tmp_paper = {}
                tmp = line.replace("#*", "", 1).replace('\n', '') # remove prefix
                if tmp:
                    tmp_paper["title"] = tmp
            if line.startswith("#@"):
                tmp = line.replace("#@", "", 1).replace('\n', '') # remove prefix
                if tmp:
                    tmp_paper["authors"] = tmp
            if line.startswith("#year"):
                tmp = line.replace("#year", "", 1).replace('\n', '') # remove prefix
                if tmp:
                    tmp_paper["year"] = tmp
            if line.startswith("#conf"):
                tmp = line.replace("#conf", "", 1).replace('\n', '') # remove prefix
                if tmp:
                    tmp_paper["venue"] = tmp
            if line.startswith("#index"):
                tmp = line.replace("#index", "", 1).replace('\n', '') # remove prefix
                if tmp:
                    tmp_paper["id"] = tmp
            if line.startswith("#%"):
                if "references" not in tmp_paper:
                    tmp_paper["references"] = []
                tmp = line.replace("#%", "", 1).replace('\n', '')
                if tmp:
                    tmp_paper["references"].append(tmp) # remove prefix
            if line.startswith("#!"):
                tmp = line.replace("#!", "", 1).replace('\n', '') # remove prefix
                if tmp:
                    tmp_paper["abstract"] = tmp
        handle_paper(tmp_paper)


def build_cumulated_years(folder, out_dir):
    onlyfiles = [f for f in listdir(folder) if isfile(join(folder, f))]
    sorted_files = sorted(onlyfiles)

    prev_files = []
    for file in sorted_files:
        out_file = open(join(out_dir, 'cumulated_' + file), 'w')
        for prev_file in prev_files:
            with open(join(folder, prev_file), 'r') as content_file:
                out_file.write(content_file.read())
        with open(join(folder, file), 'r') as content_file:
            out_file.write(content_file.read())
        prev_files.append(file)

## For V5,V6
out_file = open('arnetminer-citation-network_v6.nq', 'w')
parse_file("resources/arnetminer/raw/citation-network_v6/acm_output.txt", export_paper)
#parse_file("resources/arnetminer/raw/citation-network_v2/citation-network2.txt", export_paper)
# for key in stats:
#     print(key+"," + str(len(stats[key])))
#build_cumulated_years("resources/citation-network1/yearly-graphs", "resources/citation-network1/cumulated-graphs")
