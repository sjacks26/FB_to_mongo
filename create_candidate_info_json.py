import os
import json
import config as cfg

candidate_info_json_file = cfg.candidate_info_json_file
base_dirc = cfg.base_dirc


def resolve_key_id(data):
    """
    For some reason, some FB data uses "page_id" and some uses "id" for the id number for the page it comes from.
     This function prevents errors stemming from this.
    """
    key_id = ''
    keys = list(data.keys())
    if "id" in keys:
        key_id = "id"
    elif "page_id" in keys:
        key_id = "page_id"
    return key_id


def generate_candidate_names_dict():
    """
    This function automatically generates the dict object used to associate files with particular candidates.
    """
    dir = os.path.join(base_dirc, "download")
    page_files = []
    for path, dirs, files in os.walk(dir):
        for f in files:
            if "page" in f:
                page_files.append(os.path.join(path, f))
    candidate_names = {}
    with open(candidate_info_json_file, 'w') as o:
        for f in page_files:
            try:
                with open(f, 'r') as i:
                    page_file_contents = json.load(i)
            except ValueError:
                with open(f, 'r', encoding='latin-1') as i:
                    page_file_contents = json.load(i)
                key_id = resolve_key_id(page_file_contents)
                candidate_names[int(page_file_contents[key_id])] = page_file_contents['name']
                write_line = '{0}: {1}\n'.format(int(page_file_contents[key_id]), page_file_contents['name'])
                o.write(write_line)


generate_candidate_names_dict()