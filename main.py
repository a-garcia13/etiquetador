import streamlit as st
import pymongo
import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId
import random
import threading
import queue
import time

BATCH_SIZE = 10
query = {
    "Desc_Noticia_Limpia": {"$exists": True},
    "es_economica_manual": {"$exists": False},
    "Max_similarity": {"$exists": True, "$gt": 0.5}
}
# Connect to the MongoDB database and get the collection
client = MongoClient(
    "mongodb://inst-newstic:7E69wh96tzcKjK5u3tnFHK7BwbpT2dbU61JsXxVsYdPNTuazAGNBZQPxNo6xaQcDJbxlsIKmiDrhACDbDy1fmg%3D%3D@inst-newstic.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@inst-newstic@")
db = client["newstic"]
collection = db["TIE_Modelo_Economia"]

if 'num' not in st.session_state:
    st.session_state.num = 1
if 'data' not in st.session_state:
    st.session_state.data = []

# Create a queue for updates
update_queue = queue.Queue()


@st.cache(suppress_st_warning=True, allow_output_mutation=True)
def get_data():
    cursor = collection.find(query)
    df = pd.DataFrame(list(cursor))
    return df


# Get a batch of random news articles from the MongoDB collection
def get_random_news_batch(batch_size, df):
    random_indices = random.sample(range(len(df)), batch_size)
    random_news_batch = df.iloc[random_indices]
    return random_news_batch


# Update the es_economica_manual field in MongoDB
def update_news_status(news_id, answer):
    to_update = collection.find_one({"_id": ObjectId(news_id)})
    if "es_economica_manual" not in to_update:
        collection.update_one({"_id": ObjectId(news_id)}, {"$set": {"es_economica_manual": answer}})


def refresh_dataframe():
    cursor = collection.find(query)
    df = pd.DataFrame(list(cursor))
    return df


def process_updates():
    while True:
        news_id, answer = update_queue.get()
        update_news_status(news_id, answer)
        update_queue.task_done()


class NewArticle:
    def __init__(self, article, id, url, fecha):
        st.markdown("Fecha de la noticia: "+fecha+"\n")
        st.markdown("Fuente: "+url+"\n")
        st.write(article)
        # Display Yes and No buttons
        self.id = id
        self.yes_button = st.form_submit_button("Sí")
        self.no_button = st.form_submit_button("No")


# Start a background thread to process updates
update_thread = threading.Thread(target=process_updates, daemon=True)
update_thread.start()


def main():
    placeholder = st.empty()
    placeholder2 = st.empty()
    last_refresh_time = time.time()
    df = get_data()  # Fetch the data using the cached function

    # Fetch the first batch of random news articles
    news_batch = get_random_news_batch(BATCH_SIZE, df)
    news_batch_index = 0

    while True:
        current_time = time.time()
        if current_time - last_refresh_time >= 1800:  # 3600 seconds = 1 hour
            with st.spinner("Refreshing the data..."):
                df = refresh_dataframe()
                last_refresh_time = current_time
                news_batch = get_random_news_batch(BATCH_SIZE, df)
                news_batch_index = 0

        num = st.session_state.num

        if placeholder2.button('Finalizar sesión', key=num):
            placeholder2.empty()
            df = pd.DataFrame(st.session_state.data)
            st.dataframe(df)
            break
        else:
            with placeholder.form(key=str(num)):
                # Get a random news article and display it
                with st.spinner("Cargando articulo nuevo..."):
                    if news_batch_index >= len(news_batch):
                        news_batch = get_random_news_batch(BATCH_SIZE, df)
                        news_batch_index = 0

                    article = news_batch.iloc[news_batch_index]
                    news_batch_index += 1

                news = NewArticle(article['Desc_Noticia'], article['_id'], article['Cod_Url'], str(article['Fecha_Noticia']))
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
st.write(" Seleccione 'finalizar' para terminar la sesión")
main()
