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

BASE32ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUV"


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
    [r.wait() for r in res]
    pool.close()
    pool.join()

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


    region_encode = int("".join(map(str, face_region)))
    region_encode_base32 = int2base32(region_encode)

    output_name = image_path.with_name(
        f"{image_path.stem}-{region_encode_base32}.png"
    ).name
    output_path = tag_folder_path / output_name

    if (not overwrite) and output_path.exists():
        return output_path

    print(f"Extracting: {image_path}")
    image: Image.Image = open_image(image_path)

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
    """Wrapper of PIL.Image.open for JXL and Avif support
    Opens supported by pillow formats directly.
    Will try to load image using subprocess (djxl/avifdec)
    # Will try to load JXL images via jxlpy
    # (NOTE  jxlpy is ?not? working with latest libjxl as for 05.2022)

    Args:
        path (Path): Path of image

    Returns:
        Image: PIL Image
    """
    # try:
    #     from jxlpy import JXLImagePlugin
    # except ImportError:
    #     JXLImagePlugin = None

    try:
        if path.name.endswith(".jxl"):
            # if JXLImagePlugin:
            #     try:
            #         # ?memory leaks?
            #         img = Image.open(path)
            #         img.load()
            #         return img
            #     except OSError:
            #         pass
            img = open_image_by_cmd(path, "djxl")
        elif path.name.endswith(".avif"):
            img = open_image_by_cmd(path, "avifdec -d 8 --png-compress 0")
        else:
            return Image.open(path).convert("RGB")
        return img
    except Exception as e:
        print(path)
        print(e)
        exit()


def open_image_by_cmd(path: Path, cmd: str):
    """Load image as BytesIO using shell command

    Args:
        path (Path): Path of image
        cmd (str): Decoder command

    Returns:
        Image: PIL Image
    """
    with tempfile.NamedTemporaryFile(prefix="png_", suffix=".png") as tmp:
        cmd = f'{cmd} "{path.resolve()}" "{tmp.name}"'
        proc = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
            # cmd, shell=True, stdout=sys.stdout, stderr=sys.stderr
        )
        proc.communicate()
        tmp.seek(0)
        img = BytesIO(tmp.read())
        image = Image.open(img)
        image.load()
        img.close()
        return image


def int2base32(x: int) -> str:
    res = ""
    while True:
        if x < 32:
            res += BASE32ALPHABET[x]
            break
        else:
            res += BASE32ALPHABET[x % 32]
            x //= 32
    return res[::-1]


if __name__ == "__main__":
    main()
