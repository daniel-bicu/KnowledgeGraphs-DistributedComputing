import csv
import json
import re
from collections import defaultdict

def generate_cso_canonical_hierarchy(cso_file_path):
    def extract_topic(uri):
        match = re.search(r'/topics/([^>"]+)', uri)
        return match.group(1) if match else None

    # Maps for synonym resolution
    same_as_map = defaultdict(set)
    preferential_map = {}

    # Storage
    super_topic_map = defaultdict(list)

    with open(cso_file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) != 3:
                continue
            
            subj_raw, pred_raw, obj_raw = [x.strip() for x in row]
            subj = extract_topic(subj_raw)
            obj = extract_topic(obj_raw)
            
            if not subj or not obj:
                continue
            
            if "superTopicOf" in pred_raw:
                super_topic_map[subj].append(obj)
            elif "relatedEquivalent" in pred_raw:
                same_as_map[subj].add(obj)
                same_as_map[obj].add(subj)
            elif "preferentialEquivalent" in pred_raw:
                preferential_map[subj] = obj

    # Resolve all synonyms through preferential labels
    canonical_map = {}

    # First use preferentialEquivalent
    for syn, pref in preferential_map.items():
        canonical_map[syn] = pref

    # Then fill in relatedEquivalent for those not covered
    for key, vals in same_as_map.items():
        for v in vals:
            if v not in canonical_map:
                canonical_map[v] = canonical_map.get(key, key)

    # Normalize the superTopicOf structure by canonical topic names
    canonical_hierarchy = defaultdict(list)

    for parent, children in super_topic_map.items():
        canonical_parent = canonical_map.get(parent, parent)
        for child in children:
            canonical_child = canonical_map.get(child, child)
            if canonical_child != canonical_parent and canonical_child not in canonical_hierarchy[canonical_parent]:
                canonical_hierarchy[canonical_parent].append(canonical_child)

    # Save output
    output_path = "../../data/cso_hierarchy_canonical.json"
    with open(output_path, "w") as f:
        json.dump(canonical_hierarchy, f, indent=2)

    return output_path
