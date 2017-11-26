#!/usr/bin/python

import sys
import talend
import json

def main():
    # Arguments: repo_dir commit_id item_path
    if len(sys.argv) != 4:
        print("Usage: review.py <REPO> <COMMIT ID> <ITEM>")
        exit(1)


    path = sys.argv[1]
    commit_id = sys.argv[2]
    item_path = sys.argv[3]


    proj = talend.TalendProject(path, ref=commit_id)

    if item_path == "ALL":
        # review all!
        rev = proj.review_with_info(None, False)

    elif item_path == "LIST":
        # just list

        rev = [j.name for j in proj]
    elif item_path == "TREE":
        rev = proj.tree_view(None)

    else:
        # review particular item
        rev = proj.review_with_info(item_path)

    print(json.dumps(rev))

main()
