import pymongo
from bson.objectid import ObjectId
from pymongo import MongoClient

# Connect to the MongoDB database and get the collection
client = MongoClient(
    "mongodb://inst-newstic:7E69wh96tzcKjK5u3tnFHK7BwbpT2dbU61JsXxVsYdPNTuazAGNBZQPxNo6xaQcDJbxlsIKmiDrhACDbDy1fmg%3D%3D@inst-newstic.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@inst-newstic@")
db = client["newstic"]
collection = db["TIE_Modelo_Economia"]

# Replace the following with the document ID you want to query
DOCUMENT_ID = "643cdf29589a5768ae55e051"

# Query the collection by the document ID and print the result
document = collection.find_one({"_id": ObjectId(DOCUMENT_ID)})

if document:
    print(document['Desc_Noticia'])
else:
    print(f"No document found with the ID: {DOCUMENT_ID}")