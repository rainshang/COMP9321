import requests
import json
from pymongo import MongoClient

API_key = "&key=AIzaSyB4x8PJO2adnI_tjpv3dAOBXD-5buVnQlY"
url = "https://maps.googleapis.com/maps/api/place/photo?maxwidth=400&photoreference="
default_url = "https://maps.googleapis.com/maps/api/place/photo?maxwidth=600&photoreference=CmRaAAAAv6tCMQP8npsrCX8k3zgAksrji07t7eZ68-0TsmP4xk1mAb3U30LTMEKiqsDpS32MwT_VwtBTXhSePHbBHWpM1FaSYc15jfmNSQIdmzebybuur3W-ez6CQQsGivVbJ0UkEhBIwlhPLijl6NJAtHi4ARR9GhQ0cNqzmq3m5w3xVFMQ4_1GtnFLUg&key=AIzaSyB4x8PJO2adnI_tjpv3dAOBXD-5buVnQlY"

class DataCleanser(object):
    """docstring for DataCleanser."""
    def __init__(self):
        super(DataCleanser, self).__init__()

    def processPrediction(self, prediction, db):

        photoLib = db['suburb'].find_one({"title":"Victoria"})['suburbImage']

        counter = 0
        anchor = prediction[0]
        anchor['photo'] = photoLib[prediction[0]['suburb']]['photo']
        anchor['price'] = round(prediction[0]['price'],0)
        recommendation = []

        for p in prediction:

            if p == anchor:
                continue
            if (counter == 5):
                break
            if (p['price'] <= anchor['price'] + 20000 and p['price'] >= anchor['price'] - 20000 and photoLib.get(p['suburb'],"") != ""):
                p_ = p
                p_['photo'] = photoLib[p['suburb']]['photo']
                p_['price'] = round(p['price'],0)

                recommendation.append(p_)
                counter += 1

        return {'main':anchor,'recommendation':recommendation}

    def processRestaurant(self, dataList):
        dataList_1 = []

        for r in dataList:
            keyRating = r.get("rating","")
            keyPhoto = r.get("photos","")
            if (keyRating != "" and keyPhoto!= ""):
                dataList_1.append(r)

        sortedDataList = sorted(dataList_1, key=lambda k: k['rating'], reverse=True)

        result = []
        counter = 0

        for r in sortedDataList:

            obj = {}
            obj['name'] = r['name']
            obj['location'] = r['geometry']['location']
            obj['photo'] = url+r['photos'][0]['photo_reference']+API_key
            obj['rating'] = r['rating']
            obj['vicinity'] = r['vicinity']

            result.append(obj)
            counter += 1
            if (counter==len(sortedDataList) or counter == 4):
                break

        return result

    def processHospital(self, dataList):

        result = []
        counter = 0

        for r in dataList:

            obj = {}
            obj['name'] = r['name']
            obj['location'] = r['geometry']['location']
            obj['vicinity'] = r['vicinity']

            result.append(obj)
            counter += 1
            if (counter==len(dataList) or counter == 4):
                break

        return result

    def processSchool(self, dataList):
        dataList_1 = []

        for r in dataList:
            keyRating = r.get("rating","")
            keyPhoto = r.get("photos","")
            if (keyRating != "" and keyPhoto!= ""):
                dataList_1.append(r)

        sortedDataList = sorted(dataList_1, key=lambda k: k['rating'], reverse=True)

        result = []
        counter = 0

        for r in sortedDataList:

            obj = {}
            obj['name'] = r['name']
            obj['location'] = r['geometry']['location']
            obj['photo'] = url+r['photos'][0]['photo_reference']+API_key
            obj['rating'] = r['rating']
            obj['vicinity'] = r['vicinity']

            result.append(obj)
            counter += 1
            if (counter==len(sortedDataList) or counter == 4):
                break

        return result

    def processSupermarket(self, dataList):
        dataList_1 = []

        for r in dataList:
            keyRating = r.get("rating","")
            keyPhoto = r.get("photos","")
            if (keyRating != "" and keyPhoto!= ""):
                dataList_1.append(r)

        sortedDataList = sorted(dataList_1, key=lambda k: k['rating'], reverse=True)

        result = []
        counter = 0

        for r in sortedDataList:

            obj = {}
            obj['name'] = r['name']
            obj['location'] = r['geometry']['location']
            obj['photo'] = url+r['photos'][0]['photo_reference']+API_key
            obj['rating'] = r['rating']
            obj['vicinity'] = r['vicinity']

            result.append(obj)
            counter += 1
            if (counter==len(sortedDataList) or counter == 4):
                break

        return result

    def processSuburb(self, db):

        suburb = db['suburb']

        if (suburb.find_one({"title":"Victoria"}) == None):

            records = db['records']
            melbourne_house = records.find_one({"title": "melbourne_housing"})
            count = 1;
            suburbObject = {}
            imageObject = {}
            existedSuburb = []

            for entry in melbourne_house['entry']:

                if (entry['Postcode'] in existedSuburb):
                    continue


                existedSuburb.append(entry['Postcode'])
                suburbObject[str(count)] = {
                    "suburb"    : entry['Suburb'],
                    "postcode"  : entry['Postcode']
                }
                count += 1


                resultPhoto = requests.get("https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input="+entry['Suburb']+",Victoria"+"&inputtype=textquery&fields=photos&key=AIzaSyB4x8PJO2adnI_tjpv3dAOBXD-5buVnQlY")
                dataPhoto = json.loads(resultPhoto.content)

                if (len(dataPhoto['candidates'][0]) == 0):
                    imageObject[entry['Suburb']] = {
                        "photo": default_url
                    }
                else:
                    url = "https://maps.googleapis.com/maps/api/place/photo?maxwidth=600&photoreference="+dataPhoto['candidates'][0]['photos'][0]['photo_reference']+"&key=AIzaSyB4x8PJO2adnI_tjpv3dAOBXD-5buVnQlY"
                    imageObject[entry['Suburb']] = {
                        "photo": url
                    }


            suburbRecord = {
                "title": "Victoria",
                "entry": suburbObject,
                "suburbImage": imageObject
            }

            suburb.insert_one(suburbRecord)
            return suburbObject

        else :
            record = suburb.find_one({"title":"Victoria"})['entry']
            return record

