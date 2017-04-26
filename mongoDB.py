from pymongo import MongoClient
from pymongo.database import Collection
from typing import Dict, Any

# Constants
DATABASE_NAME = 'CBDE'
COLLECTION_NAME = 'LineItem'

# Type aliases
Json = Dict[str, Any]


def initial_message() -> None:
    print("##############################################################################")
    print("                        DOCUMENT STORES: MONGODB                              ")
    print("##############################################################################")
    print()
    print("This program creates a toy example of TPC-H database and implements some queries")
    print()
    print("##############################################################################")
    print()


def create_region(data: dict) -> Json:

    return {
        'regionkey': data['region_key'],
        'name': data['region_name'],
        'comment': "region_comment"
    }


def create_nation(data: dict) -> Json:

    return {
        'nation_key': data['nation_key'],
        'name': data['nation_name'],
        'comment': "nation_comment",
        'region': create_region(data)
    }


def create_part(data: dict) -> Json:

    return {
        'partkey': data['part_key'],
        'name': "part_name",
        'mfgr': "part_mfgr",
        'brand': "part_brand",
        'type': data['part_type'],
        'size': data['part_size'],
        'container': "part_container",
        'retailprice': 0.9,
        'comment': "part_comment"
    }


def create_supplier(data: dict) -> Json:

    return {
        'suppkey': data['supplier_key'],
        'name': "supplier_name",
        'address': "supplier_address",
        'nation': create_nation(data),
        'phone': "supplier_phone",
        'acctbal': 102.3,
        'comment': "supplier_comment"
    }


def create_partsupp(data: dict) -> Json:

    return {
        'part': create_part(data),
        'supplier': create_supplier(data),
        'availqty': 10,
        'supplycost': 10.0,
        'comment': "partsupp_comment"
    }


def create_customer(data) -> Json:

    return {
        'custkey': data['customer_key'],
        'name': "customer_name",
        'address': "customer_address",
        'nation': create_nation(data),
        'phone': "customer_phone",
        'acctbal': 1233.32,
        'mktsegment': data['customer_mktsegment'],
        'comment': "customer_comment"
    }


def create_order(data: dict) -> Json:

    return {
        'orderkey': data['order_key'],
        'customer': create_customer(data),
        'orderstatus': "order_status",
        'totalprice': 2.2,
        'orderdate': data['orderdate'],
        'orderpriority': "order_orderpriority",
        'clerk': "order_clerk",
        'shippriority': data['shippriority'],
        'comment': "order_comment"
    }


def create_lineitem(data: dict) -> Json:

    return {
        '_id': "{}_{}_{}".format(data['order_key'], data['part_key'], data['supplier_key']),
        'order': create_order(data),
        'partsupp': create_partsupp(data),
        'linenumber': 5,
        'quantity': 1.5,
        'extendedprice': 100.1,
        'discount': 0.5,
        'tax': 1.4,
        'returnflag': "N",
        'linestatus': "Y",
        'shipdate': data['shipdate'],
        'commitdate': "2014-02-09T10:50:42.389Z",
        'receiptdate': "2014-02-09T10:50:42.389Z",
        'shipinstruct': "lineitem_shipinstruct",
        'shipmode': "lineitem_shipmode",
        'comment': "lineitem_comment"
    }


def create_database() -> Collection:

    print("Cleaning database {}".format(DATABASE_NAME))

    client = MongoClient()
    client.drop_database(DATABASE_NAME)

    print("Creating collection {}".format(COLLECTION_NAME))

    db = client.get_database(DATABASE_NAME)
    return db.get_collection(COLLECTION_NAME)


def insert_data(collection: Collection) -> None:

    print("Inserting the following documents")

    data1 = {'region_key': 1,
             'region_name': "Europe",
             'nation_key': 1,
             'nation_name': "Spain",
             'part_key': 1,
             'part_type': "TYPE",
             'part_size': 10,
             'supplier_key': 1,
             'customer_key': 1,
             'customer_mktsegment': "YOUNG",
             'order_key': 1,
             'orderdate': "2014-02-09T00:00:00.000Z",
             'shipdate': "2014-02-09T00:00:00.000Z",
             'shippriority': 1
             }

    data2 = {'region_key': 1,
             'region_name': "Europe",
             'nation_key': 2,
             'nation_name': "Spain",
             'part_key': 3,
             'part_type': "TYPE",
             'part_size': 5,
             'supplier_key': 4,
             'customer_key': 5,
             'customer_mktsegment': "YOUNG",
             'order_key': 6,
             'orderdate': "2014-02-09T00:00:00.000Z",
             'shipdate': "2014-02-09T00:00:00.000Z",
             'shippriority': 1
             }

    doc1 = create_lineitem(data1)
    print(doc1)

    doc2 = create_lineitem(data2)
    print(doc2)

    collection.insert_many([doc1, doc2])

    print("{} documents were inserted".format(collection.count()))


def main():
    initial_message()
    collection = create_database()
    insert_data(collection)


if __name__ == "__main__":
    main()
