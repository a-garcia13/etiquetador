import streamlit as st
import pymongo
import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId
import random

# Connect to the MongoDB database and get the collection
client = MongoClient(
    "mongodb://inst-newstic:7E69wh96tzcKjK5u3tnFHK7BwbpT2dbU61JsXxVsYdPNTuazAGNBZQPxNo6xaQcDJbxlsIKmiDrhACDbDy1fmg%3D%3D@inst-newstic.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@inst-newstic@")
db = client["newstic"]
collection = db["Stg_TIE_Noticia_Twitter"]

if 'num' not in st.session_state:
    st.session_state.num = 1
if 'data' not in st.session_state:
    st.session_state.data = []


# Get a random news article from the MongoDB collection
def get_random_news():
    # Define the query
    query = {"es_economica_manual": {"$exists": True}}
    # Load data into a pandas DataFrame
    cursor = collection.find(query)
    df = pd.DataFrame(list(cursor))
    news_count = collection.count_documents({})
    random_news_index = random.randint(0, news_count - 1)
    return collection.find().skip(random_news_index).next()


# Update the es_economica_manual field in MongoDB
def update_news_status(news_id, answer):
    collection.update_one({"_id": ObjectId(news_id)}, {"$set": {"es_economica_manual": answer}})


class NewArticle:
    def __init__(self, article, id):
        st.markdown(article)
        # Display Yes and No buttons
        self.id = id
        self.yes_button = st.form_submit_button("Yes")
        self.no_button = st.form_submit_button("No")

def main():
    placeholder = st.empty()
    placeholder2 = st.empty()

    while True:
        num = st.session_state.num

        if placeholder2.button('end', key=num):
            placeholder2.empty()
            df = pd.DataFrame(st.session_state.data)
            st.dataframe(df)
            break
        else:
            with placeholder.form(key=str(num)):
                # Get a random news article and display it
                article = get_random_news()
                news = NewArticle(article['Desc_Noticia_Limpia'], article['_id'])
                if news.yes_button or news.no_button:
                    update_news_status(article["_id"], "Yes" if news.yes_button else "No")
                    st.session_state.data.append({
                        '_id': article['_id'], 'es_economica_manual': "Yes" if news.yes_button else "No"})
                    st.session_state.num += 1
                    placeholder.empty()
                    placeholder2.empty()
                else:
                    st.stop()


st.title("Etiquetador de noticias")
main()
