import os
import json
import config as cfg

candidate_info_json_file = cfg.candidate_info_json_file
base_dirc = cfg.base_dirc


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
            with open(f, 'r') as i:
                page_file_contents = json.load(i)
                candidate_names[int(page_file_contents['id'])] = page_file_contents['name']
                write_line = '{0}: {1}\n'.format(int(page_file_contents['id']), page_file_contents['name'])
                o.write(write_line)


generate_candidate_names_dict()