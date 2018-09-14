#!/usr/bin/python3
from flask import Flask
from flask_restplus import Api, Resource, abort, reqparse
from pymongo import MongoClient
from flask import request
import json
import requests
# from bson.objectid import ObjectId

__DEBUG__ = True

app = Flask(__name__)
api = Api(app)
mongo_client = MongoClient(
    'mongodb://admin:XmC-GvN-exM-h4p@ds028559.mlab.com:28559/wbei')
mongo_db = mongo_client['wbei']


@api.route('/<collections>')
class ImportCollection(Resource):
    def post(self, collections):
        args = None
        if not request.content_type or 'application/json' not in request.content_type:
            args = json.loads(request.data)
            if 'indicator_id' not in args:
                abort(400, "'indicator_id' missing")
        else:
            parser = reqparse.RequestParser()
            parser.add_argument('indicator_id',
                                required=True,
                                location='json',
                                type=str,
                                help='an indicator http://api.worldbank.org/v2/indicators')
            args = parser.parse_args()

        indicator_id = args['indicator_id']
        mongo_coll = mongo_db[collections]
        record = mongo_coll.find_one({'indicator': indicator_id})
        if record:
            o_id = record['_id']
            status_code = 200
        else:
            o_id = self.__fetch2mongo(mongo_coll, indicator_id)
            status_code = 201

        return {
            'location': '/{}/{}'.format(collections, o_id),
            'collection_id': str(o_id),
            'creation_time': o_id.generation_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'indicator': indicator_id
        }, status_code

    def __fetch2mongo(self, mongo_coll, indicator_id):
        __WBEI_DATA_URL = 'http://api.worldbank.org/v2/countries/all/indicators/{indicator_id}?date=2012:2017&format=json&page={page}'
        __TOTAL_PAGE = 2

        current_page = 1
        data = {}
        while current_page <= __TOTAL_PAGE:
            response = requests.get(
                __WBEI_DATA_URL.format(indicator_id=indicator_id, page=current_page))
            r_json = response.json()
            indicator_array = r_json[1]

            if not data:
                one_indicator = indicator_array[0]
                data['indicator'] = one_indicator['indicator']['id']
                data['indicator_value'] = one_indicator['indicator']['value']
                data['entries'] = []

            for one_indicator in indicator_array:
                item = {
                    'country': one_indicator['country']['value'],
                    'date': one_indicator['date'],
                    'value': one_indicator['value'],
                }
                data['entries'].append(item)

            current_page += 1

        return mongo_coll.insert_one(data).inserted_id


def main():
    app.run(host='0', debug=__DEBUG__)


if __name__ == '__main__':
    main()
