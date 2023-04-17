import streamlit as st
import pymongo
import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId
import random
import threading
import queue
import time
# Connect to the MongoDB database and get the collection
client = MongoClient(
    "mongodb://inst-newstic:7E69wh96tzcKjK5u3tnFHK7BwbpT2dbU61JsXxVsYdPNTuazAGNBZQPxNo6xaQcDJbxlsIKmiDrhACDbDy1fmg%3D%3D@inst-newstic.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@inst-newstic@")
db = client["newstic"]
collection = db["TIE_Modelo_Economia"]
query = {
    "Desc_Noticia_Limpia": {"$exists": True},
    "es_economica_manual": {"$exists": False},
    "Max_similarity": {"$exists": True}
}
cursor = collection.find(query)
df = pd.DataFrame(list(cursor))


if 'num' not in st.session_state:
    st.session_state.num = 1
if 'data' not in st.session_state:
    st.session_state.data = []

# Create a queue for updates
update_queue = queue.Queue()

# Get a random news article from the MongoDB collection
def get_random_news():
    random_index = random.randint(0, len(df) - 1)
    random_news = df.iloc[random_index]
    return random_news


# Update the es_economica_manual field in MongoDB
def update_news_status(news_id, answer):
    to_update = collection.find_one({"_id": ObjectId(news_id)})
    if "es_economica_manual" not in to_update:
        collection.update_one({"_id": ObjectId(news_id)}, {"$set": {"es_economica_manual": answer}})

def refresh_dataframe():
    global df
    cursor = collection.find(query)
    df = pd.DataFrame(list(cursor))

def process_updates():
    while True:
        news_id, answer = update_queue.get()
        update_news_status(news_id, answer)
        update_queue.task_done()

# Start a background thread to process updates
update_thread = threading.Thread(target=process_updates, daemon=True)
update_thread.start()

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
        current_time = time.time()
        if current_time - last_refresh_time >= 1800:  # 3600 seconds = 1 hour
            with st.spinner("Refreshing the data..."):
                refresh_dataframe()
                last_refresh_time = current_time

        num = st.session_state.num

        if placeholder2.button('end', key=num):
            placeholder2.empty()
            df = pd.DataFrame(st.session_state.data)
            st.dataframe(df)
            break
        else:
            with placeholder.form(key=str(num)):
                # Get a random news article and display it
                with st.spinner("Cargando articulo nuevo..."):
                    article = get_random_news()
                news = NewArticle(article['Desc_Noticia'], article['_id'])
                if news.yes_button or news.no_button:
                    update_queue.put((article["_id"], "Sí" if news.yes_button else "No"))
                    st.session_state.data.append({
                        '_id': article['_id'], 'es_economica_manual': "Sí" if news.yes_button else "No"})
                    st.session_state.num += 1
                    placeholder.empty()
                    placeholder2.empty()
                else:
                    st.stop()

st.title("Etiquetador de noticias")
st.write("Seleccione 'Sí', si la noticia es económica, de lo contrario seleccione 'No'.")
main()
