#!/usr/bin/python3
from flask import Flask, request
from flask_restplus import Resource, Api, fields
from flask_cors import CORS
from pymongo import MongoClient
from MachineLearning.Predictor import Predictor
import requests
import json
from Model.RecordReader import RecordReader
from Model.DataCleanser import DataCleanser
from Model.Plotter import Plotter
from werkzeug.datastructures import FileStorage
from OCR.ocr import *

app = Flask(__name__, static_url_path='', static_folder='PPP/build/')
CORS(app)

@app.route('/')
def index():
    return app.send_static_file('index.html')


api = Api(app, doc='/swagger/')
# SET UP MODEL for User
user = api.model(
'User',{
    'username':           fields.String,
    'password':           fields.String,
})

# set up mongodb
# SEAN mLab
MONGODB_URI = "mongodb://sean:comp4920@ds121603.mlab.com:21603/9321_asg3"
client = MongoClient(MONGODB_URI, connectTimeoutMS=30000)
db = client.get_database("9321_asg3")

# John mLab
# MONGODB_URI = "mongodb://COMP9321:comp9321password@ds117422.mlab.com:17422/comp9321"
# client = MongoClient(MONGODB_URI, connectTimeoutMS=30000)
# db = client.get_database("comp9321")


# Instantiate class
rr = RecordReader(db)
# rr.reset_mongodb("Melbourne_housing_FULL.csv")

predictor = Predictor(rr.to_dataframe("melbourne_housing"))
processor = DataCleanser()

plotter = Plotter()



"""

Predict price for a suburb, and also the price of 3 suburbs nearby. This allows user to have more choice

Also get things such as restaurants, hospitals, schools, etc, within the vincity of the suburb

"""
# query parser
queryParser = api.parser()
queryParser.add_argument('bedroom')
queryParser.add_argument('bathroom')
queryParser.add_argument('carpark')
queryParser.add_argument('type')
queryParser.add_argument('suburb')
queryParser.add_argument('floorPlan')

@api.route('/predictPrice')
@api.expect(queryParser)
class PredictPrice(Resource):
    def post(self):
        # parse query
        args        = queryParser.parse_args()

        floorPlan   = args.get('floorPlan')
        suburb      = args.get('suburb')

        if (floorPlan is not None):
            print("we have floorPlan",floorPlan)
            text_detected = detect_text(floorPlan)

            bedroom   = text_detected['bed']
            bathroom  = text_detected['bath']
            carpark   = text_detected['carspace']
            houseType = text_detected['type']

        else:
            bedroom     = args.get('bedroom')
            bathroom    = args.get('bathroom')
            carpark     = args.get('carpark')
            houseType   = args.get('type')

            print("NO FLOORPLAN")

        # get geocode
        resultLocation = requests.get("https://maps.googleapis.com/maps/api/geocode/json?address="+suburb+",Victoria"+"&key=AIzaSyB4x8PJO2adnI_tjpv3dAOBXD-5buVnQlY")
        dataLocation = json.loads(resultLocation.content)

        lat = dataLocation['results'][0]['geometry']['location']['lat']
        lng = dataLocation['results'][0]['geometry']['location']['lng']
        location = str(lat)+","+str(lng)

        # predict price
        prediction = predictor.computePrice(int(bedroom),int(bathroom),int(carpark),houseType,suburb)
        processedPrediction = processor.processPrediction(prediction, db)
        processedPrediction['main']['location'] = {
            'lat':lat,
            'lng':lng
        }

        # get near by restarant
        resultRestaurant = requests.get("https://maps.googleapis.com/maps/api/place/nearbysearch/json?location="+location+"&radius=2000&type=restaurant&key=AIzaSyB4x8PJO2adnI_tjpv3dAOBXD-5buVnQlY")
        dataRestaurant = json.loads(resultRestaurant.content)
        processedRestaurant = processor.processRestaurant(dataRestaurant['results'])

        # get near by hospital
        resultHospital = requests.get("https://maps.googleapis.com/maps/api/place/nearbysearch/json?location="+location+"&radius=2000&type=hospital&key=AIzaSyB4x8PJO2adnI_tjpv3dAOBXD-5buVnQlY")
        dataHospital = json.loads(resultHospital.content)
        processedHospital = processor.processHospital(dataHospital['results'])

        # get near by school
        resultSchool = requests.get("https://maps.googleapis.com/maps/api/place/nearbysearch/json?location="+location+"&radius=2000&type=school&key=AIzaSyB4x8PJO2adnI_tjpv3dAOBXD-5buVnQlY")
        dataSchool = json.loads(resultSchool.content)
        processedSchool = processor.processSchool(dataSchool['results'])

        # get near by supermarket
        resultSupermarket= requests.get("https://maps.googleapis.com/maps/api/place/nearbysearch/json?location="+location+"&radius=2000&type=supermarket&key=AIzaSyB4x8PJO2adnI_tjpv3dAOBXD-5buVnQlY")
        dataSupermarket = json.loads(resultSupermarket.content)
        processedSupermarket = processor.processSupermarket(dataSupermarket['results'])

        saveTrend(int(bedroom),int(bathroom),int(carpark),houseType,suburb)

        main_data = {
            "code"          : 200,
            "msg"           : "Predicted suburb successfully",
            "data"          : {
                "prediction"    : processedPrediction,
                "restaurant"    : processedRestaurant,
                "hospital"      : processedHospital,
                "school"        : processedSchool,
                "supermarket"   : processedSupermarket,
            }
        }

        return main_data, 200

@api.route('/trendRecord')
class trendRecord(Resource):
    def get(self):
        trendAnalyser = db['trendAnalyser']

        if (trendAnalyser.find_one({"total": "total"}) != None):

            totalCount = trendAnalyser.find_one({"total": "total"})

            cursor = trendAnalyser.find({})
            data   = []
            init = ["Suburb","Request"]
            data.append(init)
            suburbObject = {}

            for document in cursor:
                if (document.get('suburb',"") != ""):
                    data.append([document['suburb'],document['requestCount']])
                    suburbObject[str(document['suburb'])] = {
                        "house": document['house'],
                        "unit": document['unit'],
                    }


            return {

                "data":data,
                "options": {
                  "title": "Trend Analyser",
                  "pieHole": 0.4,
                  "is3D": "true"
                },
                "suburb": suburbObject,
                "suburbList": totalCount['suburbList']

            },200

        else:
            return {"message":"no data"},401

def saveTrend(room, bath, carpark, houseType, suburb):
    trend = db['trendAnalyser']

    if (trend.find_one({"total": "total"}) == None):
        total = {}
        total['total'] = "total"
        total['suburbList'] = [suburb]

        # insert
        trend.insert_one(total)
    else:
        total = trend.find_one({"total": "total"})

        if (suburb not in total['suburbList']):
            total['suburbList'].append(suburb)

        trend.update({"total": "total"}, total)

    suburb = suburb.lower()

    if ( trend.find_one({"suburb": suburb}) == None):

        record = {}
        record['suburb'] = suburb
        if (houseType == 'house'):
            record['house'] = {"room": room, "bath": bath, "carpark": carpark, "count": 1}
            record['unit'] = {}
        elif (houseType == 'unit'):
            record['house'] = {}
            record['unit'] = {"room": room, "bath": bath, "carpark": carpark, "count": 1}

        record['requestCount'] = 1

        # insert
        trend.insert_one(record)

        return "created"

    else:

        record = trend.find_one({"suburb": suburb})
        record['requestCount'] = int(record['requestCount']) + 1
        if (houseType == 'house'):
            if (record['house'].get("count","") != ""):
                record['house']['room']     = ( record['house']['room']     * record['house']['count'] + room )     / (record['house']['count'] + 1)
                record['house']['bath']     = ( record['house']['bath']     * record['house']['count'] + bath )     / (record['house']['count'] + 1)
                record['house']['carpark']  = ( record['house']['carpark']  * record['house']['count'] + carpark )  / (record['house']['count'] + 1)
                record['house']['count']    = ( record['house']['count'] + 1)
            else:
                record['house'] = {"room": room, "bath": bath, "carpark": carpark, "count": 1}
        elif (houseType == 'unit'):
            if (record['unit'].get("count","") != ""):
                record['unit']['room']     = ( record['unit']['room']     * record['unit']['count'] + room )     / (record['unit']['count'] + 1)
                record['unit']['bath']     = ( record['unit']['bath']     * record['unit']['count'] + bath )     / (record['unit']['count'] + 1)
                record['unit']['carpark']  = ( record['unit']['carpark']  * record['unit']['count'] + carpark )  / (record['unit']['count'] + 1)
                record['unit']['count']    = ( record['unit']['count'] + 1)
            else:
                record['unit'] = {"room": room, "bath": bath, "carpark": carpark, "count": 1}

        trend.replace_one({"suburb": suburb}, record)

        return "updated"

@api.route('/basicfilters')
class basicFilters(Resource):
    def get(self):
        return {
             "code": 200,
            "msg": "basic filters",
            "data": {
                "max_bedroom": 6,
                "max_bathroom": 6,
                "max_carspace": 4,
                "types" : [
                    "house",
                    "unit"
                ]
            }
        }, 200

@api.route('/login')
class Login(Resource):

    @api.expect(user)
    def post(self):
        data = request.json

        if (db['user'].find_one({"username":data['username']}) == None):
            return {
                "login": False,
                "msg": "User does not exist"
            }, 200

        else:
            userData = db['user'].find_one({"username":data['username']})
            if (userData['password'] == data['password']):
                return {
                    "login": True,
                }, 200
            else:
                return {
                    "login": False,
                    "msg": "wrong Password"
                }, 200

@api.route('/suburbs')
class allSuburb(Resource):
    def get(self):

        result = processor.processSuburb(db)

        return {
            "code": 200,
            "msg": "suburbs and postcodes",
            "data": result
        }, 200

@api.route('/maxprice')
class maxPrice(Resource):
    def get(self):

        return {
            "code": 200,
            "msg": "max price",
            "data": {
                "max_price": 999999
            }
        }, 200


@api.route('/distribution_transactions')
class transactions(Resource):
    def get(self):
        result = rr.groupBy("melbourne_housing","Suburb")
        return {
            "code": 200,
            "msg": "distributions all transactions",
            "data": result
        }, 200

upload_parser = api.parser()
upload_parser.add_argument('file', location='files', type=FileStorage, required=True)

@api.route('/upload')
@api.expect(upload_parser)
class Upload(Resource):
    def post(self):
        args = upload_parser.parse_args()
        uploaded_file = args['file']  # This is FileStorage instance
        print(list(args.keys()))

        detect_text(uploaded_file)

        return {'upload_status': 'done'}, 201


if __name__ == '__main__':
    # app.run(host='0', port=8007, debug=True)
    app.run(debug=True)
