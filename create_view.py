from pymongo import MongoClient

uri = 'mongodb+srv://prakul_test_3:uWkQJho8D3yUUkNc@cluster1.5ssvrrc.mongodb.net/?appName=Cluster1'
client = MongoClient(uri, serverSelectionTimeoutMS=10000)
db = client['amazon-reviews']

db.create_collection(
    'reviews_2023_33categories_100keach_Health_and_Household',
    viewOn='reviews_2023_33categories_100keach',
    pipeline=[{'$match': {'$expr': {'$eq': ['$category', 'Health_and_Household']}}}]
)
print('View created successfully.')
client.close()
