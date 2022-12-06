#!/usr/bin/env python3

from argparse import ArgumentParser
import os
import sys


def main():
    """
    Remove all files from `output-dir` not present in `faces-list` file
    """
    parser = ArgumentParser()
    parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        default="Faces",
    )
    parser.add_argument(
        "--faces-list",
        type=str,
        default="./faces.txt",
    )
    opt = parser.parse_args()

    if os.path.exists(opt.faces_list):
        with open(opt.faces_list, "r") as f:
            saved_faces: set[str] = set(f.readlines())
    else:
        sys.exit("Faces file not found")

    root_dir = "."
    for root, dirs, files in os.walk(opt.output_dir):
        for file in files:
            rel_dir = os.path.relpath(root, root_dir)
            rel_file = os.path.join(rel_dir, file)
            if (rel_file + "\n") not in saved_faces:
                print(f"NOT {rel_file}")
                os.unlink(rel_file)


if __name__ == "__main__":
    main()
