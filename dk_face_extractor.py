#!/usr/bin/env python3
import re
import sqlite3
import subprocess
import tempfile
from io import BytesIO
from math import ceil, sqrt
from multiprocessing import Pool
from pathlib import Path

from PIL import Image
from tap import Tap


class Opt(Tap):
    db: Path = Path.home() / ".config/vert/digikam4.db"
    output_dir: Path = Path("Faces")
    mount: Path = Path('/')


def main():
    opt = Opt().parse_args()
    opt.output_dir.mkdir(exist_ok=True)

    db = sqlite3.connect(opt.db)
    cur = db.cursor()

    data = fetch_data_from_db(cur, mount=opt.mount)

    pool = Pool()
    res = [pool.apply_async(save_face, (*i, opt.output_dir)) for i in data]
    for r in res:
        r.get()
    pool.close()
    pool.join()

    db.close()


def fetch_data_from_db(cur: sqlite3.Cursor, mount=Path("/")):
    """Get image_path, face_region, tag_name
    from digiKam database

    Args:
        cur (sqlite3.Cursor): Cursor for database
        mount (pathlib.Path, optional): Path to mountpoint of AlbumRoots. Defaults to Path("/").
    """
    cur.execute(
        """
SELECT substr(specificPath, 2) || relativePath || '/' || i.name,
       t.name AS TagName,
       value AS Rect
FROM
  (SELECT id,
          name
   FROM
     (SELECT tagid
      FROM TagProperties
      WHERE property == 'faceEngineId' ) fei
   JOIN Tags ON Tags.id == fei.tagid) t
JOIN ImageTagProperties ON ImageTagProperties.tagid == t.id
JOIN
  (SELECT *
   FROM Images
   JOIN Albums ON Albums.id == Images.album
   JOIN AlbumRoots ON AlbumRoots.id == Albums.albumRoot) i ON i.id == ImageTagProperties.imageid
WHERE ImageTagProperties.value like '<rect x=%'
  AND ImageTagProperties.property == 'tagRegion'
        """)

    data = []
    for image_path_without_mount, tag_name, face_region_xml in cur.fetchall():
        image_path = mount.expanduser() / image_path_without_mount
        face_region = parse_rect(face_region_xml)

        data.append((
            image_path, face_region, tag_name
        ))
    return data


def save_face(image_path, face_region, tag_name, output_dir: Path):
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
    center = (x + width/2, y + height/2)

    box = list(  # [x1, y1, x2, y2]
        map(ceil,
            (center[0] - square_width/2, center[1] - square_width/2,
             center[0] + square_width/2, center[1] + square_width/2)))

    face_crop = image.crop(box)
    face_crop_resized = face_crop.resize((224, 224))

    face_crop_resized.save(output_path, optimize=False)


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
    Will try to load JXL images via jxlpy
    (NOTE jxlpy is ?not? working with latest libjxl as for 05.2022)
    else will try to load image using subprocess (djxl/avifdec)

    Args:
        path (Path): Path of image

    Returns:
        Image: PIL Image
    """
    try:
        from jxlpy import JXLImagePlugin
    except ImportError:
        JXLImagePlugin = None

    try:
        if path.name.endswith(".jxl"):
            if JXLImagePlugin:
                try:
                    img = Image.open(path)
                    return img
                except Exception:
                    pass
            img = open_image_by_cmd(path, "djxl")
        elif path.name.endswith(".avif"):
            img = open_image_by_cmd(
                path, "avifdec -d 8 --png-compress 0")
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
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            # cmd, shell=True, stdout=sys.stdout, stderr=sys.stderr
        )
        proc.communicate()
        tmp.seek(0)
        img = BytesIO(tmp.read())
        image = Image.open(img)
        return image


if __name__ == "__main__":
    main()
