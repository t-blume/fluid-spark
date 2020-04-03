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
# "name": "Peter Kostelnik      1 ", "id": "2702511795"}
def export_author(author, out_file):
    id = author["id"]
    name = author["name"]
    uri = generate_unique_id(name, id)
    if not id in exported_author_ids:
        exported_author_ids.add(id)
        out_file.write(parse_reference(uri, "type", "Author"))
        out_file.write(parse_attribute(uri, "name", name))
        if "org" in author:
            out_file.write(parse_attribute(uri, "affiliation", author["org"]))
    return uri

def export_venue(venue, out_file):
    if "raw" not in venue:
        return None

    if "id" not in venue:
        id = fix_generator(venue["raw"])
    else:
        id = venue["id"]
    name = venue["raw"]
    uri = generate_unique_id(name, id)
    if not id in exported_venue_ids:
        exported_venue_ids.add(id)
        out_file.write(parse_reference(uri, "type", "Venue"))
        out_file.write(parse_attribute(uri, "name", name))

    return uri

def export_paper(paper, yearly_graphs, base_name):
    id = paper["id"]
    title = paper["title"]
    uri = generate_unique_id(title, id)

    year = None
    if "year" in paper and paper["year"]:
        year = str(paper["year"])

    out_file = None
    if yearly_graphs and year:
        out_file = open(base_name + '_'+ year + '.nq', 'a')

    if not yearly_graphs:
        out_file = open(base_name + '.nq', 'a')

    if not out_file:
        return

    if year:
        out_file.write(parse_attribute(uri, "year", year))

    out_file.write(parse_attribute(uri, "title", title))

    if "authors" in paper:
        for author in paper["authors"]:
            author_uri = export_author(author, out_file)
            out_file.write(parse_reference(uri, "author", author_uri))

    if "venue" in paper and paper["venue"]:
        venue_uri = export_venue(paper["venue"], out_file)
        if venue_uri is not None:
            out_file.write(parse_reference(uri, "venue", venue_uri))



    if "keywords" in paper:
        for keyword in paper["keywords"]:
            out_file.write(parse_attribute(uri, "reference", keyword))

    if "fos" in paper:
        for field in paper["fos"]:
            out_file.write(parse_attribute(uri, "field-of-study", field["name"]))

    if "references" in paper:
        for reference in paper["references"]:
            out_file.write(parse_reference(uri, "reference", reference))

    if "page_start" in paper and paper["page_start"]:
        out_file.write(parse_attribute(uri, "page_start", paper["page_start"]))

    if "page_end" in paper and paper["page_end"]:
        out_file.write(parse_attribute(uri, "page_end", paper["page_end"]))

    if "doc_type" in paper and paper["doc_type"]:
        out_file.write(parse_reference(uri, "type", paper["doc_type"]))

    if "lang" in paper and paper["lang"]:
        out_file.write(parse_attribute(uri, "lang", paper["lang"]))

    if "publisher" in paper and paper["publisher"]:
        out_file.write(parse_attribute(uri, "publisher", paper["publisher"]))

    if "volume" in paper and paper["volume"]:
        out_file.write(parse_attribute(uri, "volume", paper["volume"]))

    if "issue" in paper and paper["issue"]:
        out_file.write(parse_attribute(uri, "issue", paper["issue"]))

    if "issn" in paper and paper["issn"]:
        out_file.write(parse_attribute(uri, "issn", paper["issn"]))

    if "isbn" in paper and paper["isbn"]:
        out_file.write(parse_attribute(uri, "isbn", paper["isbn"]))

    if "doi" in paper and paper["doi"]:
        out_file.write(parse_attribute(uri, "doi", paper["doi"]))

    if "pdf" in paper and paper["pdf"]:
        out_file.write(parse_attribute(uri, "pdf", paper["pdf"]))

    if "url" in paper:
        for url in paper["url"]:
            out_file.write(parse_attribute(uri, "url", url))

    if "abstract" in paper and paper["abstract"]:
        out_file.write(parse_attribute(uri, "abstract", paper["abstract"]))

def parse_file(file, yearly_graphs, base_name, handle_paper=export_paper):
    with open(file) as file_in:
        for line in file_in:
            paper_item = json.loads(line)
            handle_paper(paper_item, yearly_graphs, base_name)


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


# parse_file("resources/arnetminer/raw/citation-network_v11/dblp_papers_v11.txt", True, "arnetminer-citation-network_v11")
# # for key in stats:
# #     print(key+"," + str(len(stats[key])))
build_cumulated_years("resources/citation-networkv11/yearly_graphs", "resources/citation-networkv11/")
