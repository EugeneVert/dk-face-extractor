#!/usr/bin/env python3
import re
import sqlite3
import subprocess
import tempfile
from argparse import ArgumentParser
from io import BytesIO
from math import ceil, sqrt
from multiprocessing import Pool
from pathlib import Path

from PIL import Image

import int2base32

try:
    import pillow_avif
except ImportError:
    pillow_avif = None

try:
    from jxlpy import JXLImagePlugin
except ImportError:
    JXLImagePlugin = None


def main():
    parser = ArgumentParser()
    parser.add_argument(
        "--db",
        type=Path,
        required=True,
        help="path to digikam4.db",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=Path("Faces"),
    )
    parser.add_argument(
        "-m",
        "--mount",
        type=Path,
        default=Path("/"),
        help="path to album mountpoint",
    )
    parser.add_argument(
        "--root",
        type=str,
        required=True,
        help="album root",
    )
    parser.add_argument(
        "--min",
        type=int,
        default=0,
        help="minimal amount of assigned faces for facetag to be extracted",
    )
    parser.add_argument(
        "-a",
        "--append-parent",
        action="store_true",
        help="append parent name of facetag to output dir name",
    )
    parser.add_argument(
        "--resize",
        type=int,
        default=0,
        help="resize extracted face regions to this size",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="reextract existing face regions",
    )
    parser.add_argument(
        "--faces-list",
        type=Path,
        default=Path("./faces.txt"),
    )

    opt = parser.parse_args()
    opt.output_dir.mkdir(exist_ok=True)

    db = sqlite3.connect(opt.db)
    cur = db.cursor()

    data = fetch_data_from_db(
        cur,
        opt.root,
        mount=opt.mount,
        append_parent_tag_name=opt.append_parent,
        min_face_count=opt.min,
    )

    pool = Pool()
    res = [
        pool.apply_async(save_face, (*i, opt.output_dir, opt.resize, opt.overwrite))
        for i in data
    ]
    saved_faces_paths = list([str(r.get()) + "\n" for r in res])
    pool.close()
    pool.join()

    if opt.faces_list.exists():
        with open(opt.faces_list, "r") as f:
            saved_faces: set[str] = set(f.readlines())
    else:
        saved_faces = set()
    saved_faces.update(saved_faces_paths)
    with open(opt.faces_list, "w") as f:
        f.writelines(saved_faces)

    db.close()


def fetch_data_from_db(
    cur: sqlite3.Cursor,
    root,
    mount=Path("/"),
    append_parent_tag_name=False,
    min_face_count=0,
):
    """Get image_path, face_region, tag_name
    from digiKam database

    Args:
        cur (sqlite3.Cursor): Cursor for database
        mount (pathlib.Path, optional): Path to mountpoint of AlbumRoots. Defaults to Path("/").
    """

    data = []

    query = """
SELECT
    substr(specificPath, 2) || relativePath || '/' || Images.name as Relpath,
    value AS Rect,
    t.name AS TagName,
    parent_tag.name as ParentTagName
FROM
    (
    SELECT
        id,
        pid,
        name
    FROM
        Tags
    JOIN ImageTagProperties ON
        ImageTagProperties.tagid == Tags.id
    GROUP BY
        tagid
    HAVING
        COUNT(tagid) >= ?
        AND ImageTagProperties.property == "tagRegion") t
JOIN ImageTagProperties ON
    ImageTagProperties.tagid == t.id
JOIN Tags AS parent_tag ON
    parent_tag.id == t.pid
JOIN Images ON
    Images.id == imageid
JOIN Albums ON
    Albums.id == Images.album
JOIN AlbumRoots ON
    AlbumRoots.id == Albums.albumRoot
WHERE AlbumRoots.label == ?
    """

    cur.execute(query, (min_face_count, root))

    for row in cur.fetchall():
        if append_parent_tag_name:
            (
                image_path_without_mount,
                face_region_xml,
                tag_name,
                parent_tag_name,
            ) = row
            # NOTE ∕ is a [Division Slash]
            tag_name = f"{parent_tag_name}∕{tag_name}"
        else:
            (
                image_path_without_mount,
                face_region_xml,
                tag_name,
                _,
            ) = row

        image_path = mount.expanduser() / image_path_without_mount
        face_region = parse_rect(face_region_xml)
        data.append((image_path, face_region, tag_name))

    return data


def save_face(
    image_path: Path,
    face_region: tuple[int, int, int, int],
    tag_name: str,
    output_dir: Path,
    resize_to: int,
    overwrite: bool = False,
):
    """
    Save face region and return file path.
    """
    tag_folder_path = output_dir / tag_name
    tag_folder_path.mkdir(exist_ok=True)

    region_base32 = int2base32.encode_region(face_region)

    output_name = image_path.with_name(f"{image_path.stem}-{region_base32}.png").name
    output_path = tag_folder_path / output_name

    if (not overwrite) and output_path.exists():
        return output_path

    print(f"Extracting: {image_path}")
    image: Image.Image = open_image(image_path).convert("RGB")

    x = face_region[0]
    y = face_region[1]
    width = face_region[2]
    height = face_region[3]

    square_width = ceil(sqrt(width * height))
    center = (x + width / 2, y + height / 2)
    margin = (
        square_width / 10
    )  # FaceUtils::faceRectDisplayMargin:utilities/facemanagement/database/faceutils.cpp 438

    x1 = center[0] - square_width / 2 - margin
    y1 = center[1] - square_width / 2 - margin
    x2 = center[0] + square_width / 2 + margin
    y2 = center[1] + square_width / 2 + margin

    box = tuple(
        map(
            ceil,
            (
                x1 if x1 > 0 else 0,
                y1 if y1 > 0 else 0,
                x2 if x2 < image.size[0] else image.size[0],
                y2 if y2 < image.size[1] else image.size[1],
            ),
        )
    )

    face_crop = image.crop(box)
    image.close()

    if resize_to != 0:
        size_to = (resize_to, resize_to)
        face_crop = face_crop.resize(size_to)

    face_crop.save(output_path, optimize=False)
    face_crop.close()

    return output_path


def parse_rect(rect):
    """Get x, y, width, height from xml line

    Args:
        rect (str): String like <rect x="123" y="123" width="123" height="123"/>

    Returns:
        Turple[int, int, int, int]: x, y, width, height
    """
    RE = re.compile(r"\w*=\"(\d*)\"")
    return list(map(int, RE.findall(rect)))


def open_image(path: Path):
    """Kludge to support loading JPEG XL and AVIF images
    Args:
        path (Path): Path of image

    Returns:
        Image: PIL Image
    """
    suffix = path.suffix.lower()
    try:
        if suffix == ".jxl":
            if JXLImagePlugin:
                return Image.open(path)
            else:
                return open_image_by_cmd(
                    path,
                    ["djxl", "--num_threads=0", "--color_space=RGB_D65_SRG_Rel_SRG"],
                )

        if suffix == ".avif":
            if pillow_avif:
                return Image.open(path)
            else:
                return open_image_by_cmd(
                    path,
                    ["avifdec", "-d", "8", "--png-compress", "0"],
                )

        return Image.open(path)

    except Exception as e:
        print(path)
        print(e)
        exit()


def open_image_by_cmd(path: Path, cmd: list[str]):
    """Load image as BytesIO using shell command

    Args:
        path (Path): Path of image
        cmd (str): Decoder command

    Returns:
        Image: PIL Image
    """
    with tempfile.NamedTemporaryFile(prefix="png_", suffix=".png") as tmp:
        cmd.append(str(path.resolve()))
        cmd.append(tmp.name)

        try:
            subprocess.run(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            ).check_returncode()
        except subprocess.CalledProcessError as e:
            print(f"Subprocess '{e.cmd}' returned non-zero error code: {e.returncode}")
            print(e.stderr)
            exit(1)

        tmp.seek(0)
        img = BytesIO(tmp.read())
        image = Image.open(img)
        image.load()
        img.close()
        return image


if __name__ == "__main__":
    main()
