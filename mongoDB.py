from pymongo import MongoClient
from pymongo.database import Collection, CommandCursor
from typing import Dict, Any

# Constants
DATABASE_NAME = 'CBDE'
COLLECTION_NAME = 'LineItem'

# Type aliases
Json = Dict[str, Any]


def initial_message() -> None:
    print("################################################################################")
    print("                        DOCUMENT STORES: MONGODB                              ")
    print("################################################################################")
    print()
    print("This program creates a toy example of TPC-H database and implements some queries")
    print()
    print("################################################################################")
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
        'extendedprice': 100.0,
        'discount': 0.5,
        'tax': 0.5,
        'returnflag': data['returnflag'],
        'linestatus': data['linestatus'],
        'shipdate': data['shipdate'],
        'commitdate': "2014-02-09T10:50:42.389Z",
        'receiptdate': "2014-02-09T10:50:42.389Z",
        'shipinstruct': "lineitem_shipinstruct",
        'shipmode': "lineitem_shipmode",
        'comment': "lineitem_comment"
    }


def create_database() -> Collection:

    print("Cleaning database {}\n".format(DATABASE_NAME))

    client = MongoClient()
    client.drop_database(DATABASE_NAME)

    print("Creating collection {}\n".format(COLLECTION_NAME))

    db = client.get_database(DATABASE_NAME)
    return db.get_collection(COLLECTION_NAME)


def insert_data(collection: Collection) -> None:

    print("Inserting the following documents")

    data1 = {'region_key': 1,
             'region_name': "Europe",
             'nation_key': 1,
             'nation_name': "Spain",
             'part_key': 1,
             'part_type': "Round",
             'part_size': 10,
             'supplier_key': 1,
             'customer_key': 1,
             'customer_mktsegment': "YOUNG",
             'order_key': 1,
             'orderdate': "2014-01-01T00:00:00.000Z",
             'returnflag': "A",
             'linestatus': "A",
             'shipdate': "2014-01-01T00:00:00.000Z",
             'shippriority': 1
             }

    data2 = {'region_key': 1,
             'region_name': "Europe",
             'nation_key': 2,
             'nation_name': "France",
             'part_key': 2,
             'part_type': "Square",
             'part_size': 10,
             'supplier_key': 2,
             'customer_key': 2,
             'customer_mktsegment': "YOUNG",
             'order_key': 2,
             'orderdate': "2014-01-01T00:00:00.000Z",
             'returnflag': "A",
             'linestatus': "A",
             'shipdate': "2014-01-01T00:00:00.000Z",
             'shippriority': 1
             }

    data3 = {'region_key': 1,
             'region_name': "Europe",
             'nation_key': 2,
             'nation_name': "Spain",
             'part_key': 3,
             'part_type': "Rigid",
             'part_size': 5,
             'supplier_key': 3,
             'customer_key': 5,
             'customer_mktsegment': "YOUNG",
             'order_key': 3,
             'orderdate': "2016-01-01T00:00:00.000Z",
             'returnflag': "B",
             'linestatus': "A",
             'shipdate': "2014-01-01T00:00:00.000Z",
             'shippriority': 1
             }

    data4 = {'region_key': 2,
             'region_name': "Asia",
             'nation_key': 2,
             'nation_name': "Spain",
             'part_key': 4,
             'part_type': "Round",
             'part_size': 10,
             'supplier_key': 4,
             'customer_key': 5,
             'customer_mktsegment': "OLD",
             'order_key': 4,
             'orderdate': "2014-01-01T00:00:00.000Z",
             'returnflag': "B",
             'linestatus': "A",
             'shipdate': "2020-01-01T00:00:00.000Z",
             'shippriority': 1
             }

    data = [data1, data2, data3, data4]

    for datum in data:
        document = create_lineitem(datum)
        collection.insert_one(document)
        print(document)

    print("\n{} documents were inserted\n".format(collection.count()))


def create_indexes(collection: Collection) -> None:

    collection.create_index('shipdate')
    collection.create_index('partsupp.supplier.nation.region.name')
    collection.create_index('order.customer.mktsegment')
    collection.create_index('order.orderdate')


def query1(collection: Collection, date: str) -> CommandCursor:

    return collection.aggregate([
        {"$match": {
            "shipdate": {"$lte": date}
        }},
        {"$project": {
            "l_returnflag": "$returnflag",
            "l_linestatus": "$linestatus",
            "l_quantity": "$quantity",
            "l_extendedprice": "$extendedprice",
            "l_discount": "$discount",
            "l_tax": "$tax"
        }},
        {"$group": {
            "_id": {"l_returnflag": "$l_returnflag", "l_linestatus": "$l_linestatus"},
            "l_returnflag": {"$first": "$l_returnflag"},
            "l_linestatus": {"$first": "$l_linestatus"},
            "sum_qty": {"$sum": "$l_quantity"},
            "sum_base_price": {"$sum": "$l_extendedprice"},
            "sum_disc_price": {"$sum": {"$multiply": ["$l_extendedprice", {"$subtract": [1, "$l_discount"]}]}},
            "sum_charge": {"$sum": {
                "$multiply": [{"$multiply": ["$l_extendedprice", {"$subtract": [1, "$l_discount"]}]},
                              {"$add": [1, "$l_tax"]}]}},
            "avg_qty": {"$avg": "$l_quantity"},
            "avg_price": {"$avg": "$l_extendedprice"},
            "avg_disc": {"$avg": "$l_discount"},
            "count_order": {"$sum": 1}
        }},
        {"$sort": {
            "l_returnflag": 1,
            "l_linestatus": 1
        }}
    ])


def query2(collection: Collection, part_size: int, part_type: str, region: str) -> CommandCursor:

    return collection.aggregate([
        {"$match": {
            "$and": [
                {"partsupp.part.size": {"$eq": part_size}},
                {"partsupp.part.type": {"$regex": part_type}},
                {"partsupp.supplier.nation.region.name": {"$eq": region}},
                {"partsupp.supplycost": {"$eq": query2_subquery(collection, region)}}
            ]
        }},
        {"$project": {
            "s_acctbal": "$partsupp.supplier.acctbal",
            "s_name": "$partsupp.supplier.name",
            "n_name": "$partsupp.supplier.nation.name",
            "p_partkey": "$partsupp.part.partkey",
            "p_mfgr": "$partsupp.part.mfgr",
            "s_address": "$partsupp.supplier.address",
            "s_phone": "$partsupp.supplier.phone",
            "s_comment": "$partsupp.supplier.comment"
        }},
        {"$sort": {
            "s_acctbal": -1,
            "n_name": 1,
            "s_name": 1,
            "p_partkey": 1
        }}
    ])


def query2_subquery(collection: Collection, region: str) -> float:
    return collection.aggregate([
        {"$match": {
            "partsupp.supplier.nation.region.name": {"$eq": region}
        }},
        {"$project": {
            "ps_supplycost": "$partsupp.supplycost"
        }},
        {"$group": {
            "_id": None,
            "min_supplycost": {"$min": "$ps_supplycost"}
        }}
    ]).next()['min_supplycost']


def query3(collection: Collection, market_segment: str, order_date: str, ship_date: str):
    return collection.aggregate([
        {"$match": {
            "$and": [
                {"order.customer.mktsegment": {"$eq": market_segment}},
                {"order.orderdate": {"$lt": order_date}},
                {"shipdate": {"$gt": ship_date}}
            ]
        }},
        {"$project": {
            "l_orderkey": "$order.orderkey",
            "l_extendedprice": "$extendedprice",
            "l_discount": "$discount",
            "o_orderdate": "$order.orderdate",
            "o_shippriority": "$order.shippriority"
        }},
        {"$group": {
            "_id": {"l_orderkey": "$l_orderkey", "o_orderdate": "$o_orderdate", "o_shippriority": "$o_shippriority"},
            "l_orderkey": {"$first": "$l_orderkey"},
            "revenue": {"$sum": {"$multiply": ["$l_extendedprice", {"$subtract": [1, "$l_discount"]}]}},
            "o_orderdate": {"$first": "$o_orderdate"},
            "o_shippriority": {"$first": "$o_shippriority"}
        }},
        {"$sort": {
            "revenue": -1,
            "o_orderdate": 1
        }}
    ])


def query4(collection: Collection, order_date: str, region: str):
    return collection.aggregate([
        {"$match": {
            "$and": [
                {"partsupp.supplier.nation.region.name": {"$eq": region}},
                {"order.orderdate": {"$gte": order_date}},
                {"order.orderdate": {"$lt": add_one_year(order_date)}}
            ]
        }},
        {"$project": {
            "n_name": "$partsupp.supplier.nation.name",
            "l_extendedprice": "$extendedprice",
            "l_discount": "$discount"
        }},
        {"$group": {
            "_id": {"n_name": "$n_name"},
            "n_name": {"$first": "$n_name"},
            "revenue": {"$sum": {"$multiply": ["$l_extendedprice", {"$subtract": [1, "$l_discount"]}]}}
        }},
        {"$sort": {
            "revenue": -1
        }}
    ])


def add_one_year(date: str) -> str:
    return date[:3] + str(int(date[3]) + 1) + date[4:]


def display_results(query : int, result: CommandCursor) -> None:

    print()
    print("################################################################################")
    print("                     Query {} results                             ".format(query))
    print()

    for row in result:
        print(row)

    print()


def execute_queries(collection: Collection) -> None:

    result1 = query1(collection, "2016-01-01T00:00:00.000Z")
    display_results(1, result1)

    result2 = query2(collection, 10, "R", "Europe")
    display_results(2, result2)

    result3 = query3(collection, "YOUNG", "2015-01-01T00:00:00.000Z", "2012-01-01T00:00:00.000Z")
    display_results(3, result3)

    result4 = query4(collection, "2014-01-01T00:00:00.000Z", "Europe")
    display_results(4, result4)


def main():
    initial_message()
    collection = create_database()
    insert_data(collection)
    create_indexes(collection)
    execute_queries(collection)

if __name__ == "__main__":
    main()
