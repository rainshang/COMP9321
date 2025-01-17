#!/usr/bin/python3
from flask import Flask, request
from flask_restplus import Api, Resource, fields, abort
from pymongo import MongoClient, ASCENDING, DESCENDING
import json
import requests
from bson.objectid import ObjectId
from bson.errors import InvalidId
import re

__DEBUG__ = True

app = Flask(__name__)
api = Api(app)
mongo_client = MongoClient(
    'mongodb://admin:XmC-GvN-exM-h4p@ds028559.mlab.com:28559/wbei')
mongo_db = mongo_client['wbei']


@api.route('/<collections>')
class Collections(Resource):
    @api.doc(
        body=api.model('', {
            'indicator_id': fields.String(description='http://api.worldbank.org/v2/indicators', example='NY.GDP.MKTP.CD'),
        }),
        responses={
            200: """
        {
            "location" : "/<collections>/<collection_id>",
            "collection_id" : "<collection_id>",
            "creation_time": "2018-04-08T12:06:11Z",
            "indicator" : "<indicator>"
        }""",
            201: """
        {
            "location" : "/<collections>/<collection_id>",
            "collection_id" : "<collection_id>",
            "creation_time": "2018-04-08T12:06:11Z",
            "indicator" : "<indicator>"
        }""",
            400: """
        {
            "message": "<error msg>"
        }"""
        })
    def post(self, collections):
        parser = api.parser()
        parser.add_argument('indicator_id',
                            required=True,
                            location='json',
                            type=str)
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
            if len(r_json) < 2:
                abort(400, r_json[0]['message'][0]['value'])
            else:
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

    @api.doc(
        responses={
            200: """
            [
                {
                    "location" : "/<collections>/<collection_id_1>",
                    "collection_id" : "collection_id_1",
                    "creation_time": "<time>",
                    "indicator" : "<indicator>"
                    },
                {
                    "location" : "/<collections>/<collection_id_2>",
                    "collection_id" : "collection_id_2",
                    "creation_time": "<time>",
                    "indicator" : "<indicator>"
                },
                ...
                ]""",
            400: """
            {
                "message": "<error msg>"
            }"""
        })
    def get(self, collections):
        mongo_coll = mongo_db[collections]
        cursor = mongo_coll.find({}, {
            "indicator": 1
        },)
        if cursor:
            response = []
            for record in cursor:
                o_id = record['_id']
                response.append(
                    {
                        'location': '/{}/{}'.format(collections, o_id),
                        'collection_id': str(o_id),
                        'creation_time': o_id.generation_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'indicator': record['indicator']
                    }
                )
            cursor.close()
            return response
        else:
            abort(400, 'Cannot find Collections = {} from the database!'.format(
                collections))


@api.route('/<collections>/<collection_id>')
class CollectionsCollection_id(Resource):
    @api.doc(
        responses={
            200: """
            {
                "message" :"Collection = <collection_id> is removed from the database!"
            }""",
            400: """
            {
                "message": "<error msg>"
            }"""
        })
    def delete(self, collections, collection_id):
        mongo_coll = mongo_db[collections]
        try:
            record = mongo_coll.find_one_and_delete(
                {'_id': ObjectId(collection_id)})
            if record:
                return {
                    'message': 'Collection = {} is removed from the database!'.format(collection_id)
                }
            else:
                abort(400, "Cannot find Collection = {} in '{}'".format(
                    collection_id, collections))
        except InvalidId as e:
            abort(400, e)

    @api.doc(
        responses={
            200: """
            {
                "collection_id" : "<collection_id>",
                "indicator": "NY.GDP.MKTP.CD",
                "indicator_value": "GDP (current US$)",
                "creation_time" : "<creation_time>"
                "entries" : [
                                {"country": "Arab World",  "date": "2016",
                                    "value": 2500164034395.78 },
                                {"country": "Australia",   "date": "2016",
                                    "value": 780016444034.00 },
                                ...
                ]
            }""",
            400: """
            {
                "message": "<error msg>"
            }"""
        })
    def get(self, collections, collection_id):
        mongo_coll = mongo_db[collections]
        try:
            record = mongo_coll.find_one(
                {'_id': ObjectId(collection_id)})
            if record:
                o_id = record['_id']
                return {
                    'collection_id': str(o_id),
                    'indicator': record['indicator'],
                    'indicator_value': record['indicator_value'],
                    'creation_time': o_id.generation_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'entries': record['entries']
                }
            else:
                abort(400, "Cannot find Collection = {} in '{}'".format(
                    collection_id, collections))
        except InvalidId as e:
            abort(400, e)


@api.route('/<collections>/<collection_id>/<int:year>/<country>')
class CollectionsCollection_idYearCountry(Resource):
    @api.doc(
        responses={
            200: """
            {
                "collection_id": <collection_id>,
                "indicator" : "<indicator_id>",
                "country": "<country>,
                "year": "<year">,
                "value": <indicator_value_for_the_country>
            }""",
            400: """
            {
                "message": "<error msg>"
            }"""
        })
    def get(self, collections, collection_id, year, country):
        mongo_coll = mongo_db[collections]
        try:
            cursor = mongo_coll.aggregate([
                {'$match': {'_id': ObjectId(collection_id)}},
                {'$project': {
                    'indicator': True,
                    'entries': {
                        '$filter': {
                            'input': '$entries',
                            'as': 'item',
                            'cond': {
                                '$and': [
                                    {'$eq': ['$$item.country', country]},
                                    {'$eq': ['$$item.date', str(year)]}
                                ]
                            }
                        }
                    }
                }}
            ])
            if cursor:
                for record in cursor:
                    cursor.close()
                    entries = record['entries']
                    if entries:
                        return {
                            'collection_id': str(record['_id']),
                            'indicator': record['indicator'],
                            'country': country,
                            'year': str(year),
                            'value': entries[0]['value'],
                        }
                    else:
                        abort(400, 'Record is not found from the database!')
            else:
                abort(400, 'Record is not found from the database!')
        except InvalidId as e:
            abort(400, e)


@api.route('/<collections>/<collection_id>/<int:year>')
class CollectionsCollection_idYearQuery(Resource):
    parser = parser = api.parser()
    parser.add_argument('q', store_missing=False)

    @api.doc(
        parser=parser,
        responses={
            200: """
            {
                "indicator": "NY.GDP.MKTP.CD",
                "indicator_value": "GDP (current US$)",
                "entries" : [
                                {
                                    "country": "Arab World",
                                    "date": "2016",
                                    "value": 2500164034395.78
                                },
                                ...
                            ]
            }""",
            400: """
            {
                "message": "<error msg>"
            }"""
        })
    def get(self, collections, collection_id, year):
        args = CollectionsCollection_idYearQuery.parser.parse_args()
        cursor = None
        try:
            if 'q' in args:
                q = args['q']
                match = re.match(r'^(top|bottom)(\d+)$', q)
                if match:
                    N = int(match.group(2))
                    if N > 0 and N < 101:
                        mongo_coll = mongo_db[collections]
                        sort = None
                        if match.group(1) == 'top':
                            sort = DESCENDING
                        else:
                            sort = ASCENDING

                        cursor = mongo_coll.aggregate([
                            {'$match': {'_id': ObjectId(collection_id)}},
                            {'$unwind': '$entries'},
                            {'$match': {'entries.date': str(year)}},
                            {'$sort': {'entries.value': sort}},
                            {'$limit': N},
                            {'$group': {
                                '_id': '$_id',
                                'indicator': {'$first': '$indicator'},
                                'indicator_value': {'$first': '$indicator_value'},
                                'entries': {'$push': '$entries'}
                            }}
                        ])
                    else:
                        abort(400, '<N> should be in range [1, 100]')
                else:
                    abort(400, "<query> should be 'top<N>' or 'bottom<N>'")
            else:
                mongo_coll = mongo_db[collections]
                cursor = mongo_coll.aggregate([
                    {'$match': {'_id': ObjectId(collection_id)}},
                    {'$unwind': '$entries'},
                    {'$match': {'entries.date': str(year)}},
                    {'$group': {
                        '_id': '$_id',
                        'indicator': {'$first': '$indicator'},
                        'indicator_value': {'$first': '$indicator_value'},
                        'entries': {'$push': '$entries'}
                    }}
                ])
        except InvalidId as e:
            abort(400, e)

        if cursor:
            for record in cursor:
                cursor.close()
                del record['_id']
                return record
        else:
            abort(400, 'Record is not found from the database!')


def main():
    app.run(host='0', port=8008, debug=__DEBUG__)


if __name__ == '__main__':
    main()
