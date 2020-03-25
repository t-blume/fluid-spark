import os
from os import listdir
from os.path import isfile, join
import io
import json
import string
import re

#* --- paperTitle
#@ --- Authors
#t ---- Year
#c  --- publication venue
#index 00---- index id of this paper
#% ---- the id of references of this paper (there are multiple lines, with each indicating a reference)
#! --- Abstract

input = 'arnetminer-citation-network'
exported_author_ids = set()
exported_venue_ids = set()

out_file = None


def parse_attribute(id, label, attribute):
    return "<"+id+"> <"+label+"> \""+re.sub("(<|>)", "", attribute.encode('utf-8'))+"\" <"+input+"> .\n"

def parse_reference(id, label, reference):
    return "<"+id+"> <"+label+"> <"+reference+"> <"+input+"> .\n"


stats = {}

def export_stats(paper):
    if"id" in paper:
        year = paper["year"]
        if year not in stats:
            stats[year] = set()
        stats[year].add(paper['id'])


def fix_generator(name):
    tmp = name.translate({ord(c): None for c in string.whitespace})
    tmp = tmp.lower()
    tmp = re.sub("(-|,|;|:|\.|\?)", "", tmp)
    tmp = re.sub("(a|e|i|o|u)", "", tmp)
    return hash(name)
# todo
def generate_unique_id(name, id):
    # tmp = name.translate({ord(c): None for c in string.whitespace})
    # tmp = tmp.lower()
    # tmp = re.sub("(-|,|;|:|\.|\?)", "", tmp)
    # tmp = re.sub("(a|e|i|o|u)", "", tmp)
    # return str(hash(name) + int(id))
    return str(id)


def export_paper(paper):
    id = paper["id"]
    title = paper["title"]
    uri = generate_unique_id(title, id)
    out_file.write(parse_attribute(uri, "title", title))

    if "authors" in paper:
        for author in paper["authors"]:
            out_file.write(parse_attribute(uri, "author", author))

    if "venue" in paper and paper["venue"]:
        out_file.write(parse_attribute(uri, "venue", paper["venue"]))

    if "year" in paper and paper["year"]:
        out_file.write(parse_attribute(uri, "year", str(paper["year"])))

    if "references" in paper:
        for reference in paper["references"]:
            out_file.write(parse_reference(uri, "reference", reference))

    if "abstract" in paper and paper["abstract"]:
        out_file.write(parse_attribute(uri, "abstract", paper["abstract"]))

def parse_file(file, handle_paper=export_paper):
    with open(file) as file_in:
        for line in file_in:
            paper_item = json.loads(line)
            export_paper(paper_item)


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

out_file = open('arnetminer-citation-network_v10.nq', 'w')
parse_file("resources/arnetminer/raw/citation-network_v10/dblp-ref-0.json")
parse_file("resources/arnetminer/raw/citation-network_v10/dblp-ref-1.json")
parse_file("resources/arnetminer/raw/citation-network_v10/dblp-ref-2.json")
parse_file("resources/arnetminer/raw/citation-network_v10/dblp-ref-3.json")
