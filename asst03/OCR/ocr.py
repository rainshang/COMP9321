from google.cloud import vision
import io
import os
from google.oauth2 import service_account
import re

def detect_text(img_file):
    """Detects text in the file."""

    client = vision.ImageAnnotatorClient()
    image = vision.types.Image()

    image.source.image_uri = img_file

    response = client.text_detection(image=image)
    texts = response.text_annotations

    # ====================
    # client = vision.ImageAnnotatorClient()
    # image = vision.types.Image(content=content)
    #
    # content = img_file.read()
    #
    # response = client.text_detection(image=image)
    # texts = response.text_annotations
     # ==================

    bed = -1
    bath = -1
    carspace = -1
    temp = ""
    count = 0
    dimension = []
    carcount = 0
    for text in texts:
        if("bed" in text.description.lower()):
            bed += 1

        if("bath" in text.description.lower() or "ensui" in text.description.lower()):
            bath += 1

        if("garage" in text.description.lower()):
            carspace += 1
            temp = "garage"

        elif (temp == "garage" and carspace == 1 and count < 3):

            normalizeNumber = text.description.replace("m","")
            normalizeNumber = normalizeNumber.replace("M","")

            if re.match("^\d+?\.\d+?$", normalizeNumber) is None:

                if (normalizeNumber.lower() != "x"):
                    temp = ""
                    carspace = 0
                continue

            count += 1
            dimension.append(float(normalizeNumber))

            if (len(dimension) == 2):

                area = dimension[0] * dimension [1]

                if (area < 24):
                    carcount = 1
                elif (30 <= area <= 50):
                    carcount = 2
                elif (51 <= area <= 60):
                    carcount = 3
                elif ( area >= 65):
                    carcount = 4

            print("dimension: {}".format(dimension))

            if (count == 3):
                temp = ""

    if (carspace == -1):
        carcount = 0

    if (bed == -1 ):
        bed = 0

    if (bath == -1):
        bath = 0

    print("bed: {}, bath: {}, carspace: {}".format(bed, bath, carcount))

    return {
        "bed": bed,
        "bath": bath,
        "carspace": carcount,
        "type": "house"
    }
