#!/usr/bin/env python3
import re
import sqlite3
import subprocess
import tempfile
from io import BytesIO
from math import ceil, sqrt
from multiprocessing import Pool
from pathlib import Path
from typing import Tuple

from PIL import Image
from tap import Tap


class Opt(Tap):
    db: Path = Path.home() / ".config/vert/digikam4.db"
    output_dir: Path = Path("Faces")
    mount: Path = Path("/")
    root: str
    min_face_count: int = 0
    append_parent_tag_name: bool = False
    resize: int = 0


def main():
    opt = Opt().parse_args()
    opt.output_dir.mkdir(exist_ok=True)

    db = sqlite3.connect(opt.db)
    cur = db.cursor()

    data = fetch_data_from_db(
        cur,
        opt.root,
        mount=opt.mount,
        append_parent_tag_name=opt.append_parent_tag_name,
        min_face_count=opt.min_face_count,
    )

    pool = Pool()
    res = [pool.apply_async(save_face, (*i, opt.output_dir, opt.resize)) for i in data]
    for r in res:
        r.get()
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
SELECT substr(specificPath, 2) || relativePath || '/' || i.name,
       t.name AS TagName,"""

    if append_parent_tag_name:
        query += """
       parent_tag.name AS ParentTagName, """

    query += """
       ImageTagProperties.value AS Rect
-- Get all face tags
FROM
    (SELECT id,
            pid,
            name
     FROM
         (SELECT tagid
          FROM TagProperties
          WHERE property == 'faceEngineId' ) fei
     JOIN Tags ON Tags.id == fei.tagid) t"""

    if min_face_count != 0:
        query += """
    -- Filter face tags by count
    JOIN
        (SELECT tagid
        FROM ImageTagProperties
        GROUP BY tagid
        HAVING COUNT(tagid) >= ?) itpc ON itpc.tagid == t.id"""

    query += """
    -- Get face regions and image ids with face tags
    JOIN ImageTagProperties ON ImageTagProperties.tagid == t.id"""

    if append_parent_tag_name:
        query += """
-- Get parent tag
JOIN Tags AS parent_tag ON parent_tag.id == t.pid"""

    query += """
-- Get albums and these albums roots for images
JOIN
    (SELECT *
     FROM Images
     JOIN Albums ON Albums.id == Images.album
     JOIN AlbumRoots ON AlbumRoots.id == Albums.albumRoot) i ON i.id == ImageTagProperties.imageid
WHERE ImageTagProperties.value like '<rect x=%'
    AND ImageTagProperties.property == 'tagRegion'
    """

    query += f"\
    AND label == '{root}'"

    if min_face_count:
        cur.execute(query, (min_face_count,))
    else:
        cur.execute(query)

    if append_parent_tag_name:
        for (
            image_path_without_mount,
            tag_name,
            parent_tag_name,
            face_region_xml,
        ) in cur.fetchall():
            image_path = mount.expanduser() / image_path_without_mount
            face_region = parse_rect(face_region_xml)
            tag_name = f"{parent_tag_name}∕{tag_name}"  # NOTE ∕ is a [Division Slash]

            data.append((image_path, face_region, tag_name))
        return data

    else:
        for (image_path_without_mount, tag_name, face_region_xml) in cur.fetchall():
            image_path = mount.expanduser() / image_path_without_mount
            face_region = parse_rect(face_region_xml)

            data.append((image_path, face_region, tag_name))
        return data


def save_face(image_path, face_region, tag_name, output_dir: Path, resize_to: int):
    print(image_path)
    tag_folder_path = output_dir / tag_name
    tag_folder_path.mkdir(exist_ok=True)
    output_path = tag_folder_path / (image_path.with_suffix(".png").name)
    if output_path.exists():
        return

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


if __name__ == "__main__":
    main()
