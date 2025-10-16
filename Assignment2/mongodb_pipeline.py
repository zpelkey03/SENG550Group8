from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client["sales_db"]        # replace with your DB
collection = db["orders_summary"]        # your collection name
# 1. Distinct Cities per Customer
pipeline = [
    {
        "$group": {
            "_id": "$customer_id",
            "customer_name": {"$first": "$customer_name"},
            "distinct_cities": {"$addToSet": "$customer_city"}
        }
    },
    {
        "$project": {
            "customer_name": 1,
            "num_distinct_cities": {"$size": "$distinct_cities"}
        }
    }
]

# results = list(collection.aggregate(pipeline))
# for r in results:
#     print(r)

# 2. Total Amount Spent per City
pipeline = [
    {
        "$group": {
            "_id": "$customer_city",
            "total_amount": {"$sum": "$amount"}
        }
    },
    {
        "$sort": {"total_amount": -1}
    }
]

# results = list(collection.aggregate(pipeline))
# for r in results:
#     print(r)

# 3. Sum of (listed price - paid amount)
pipeline = [
    {
        "$project": {
            "diff": {"$subtract": ["$product_price", "$amount"]}
        }
    },
    {
        "$group": {
            "_id": None,
            "total_diff": {"$sum": "$diff"}
        }
    }
]

result = collection.aggregate(pipeline)
print(list(result))