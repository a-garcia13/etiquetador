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
    "Desc_Noticia": {"$exists": True},
    "es_economica_manual": {"$exists": False},
    "Max_similarity": {"$exists": True, "$gt": 0.5, "$lt": 0.9},
    "character_count": {"$exists": True, "$gt": 500}
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

# Create a global set to store served article IDs
served_articles = set()


def filter_urls(data_frame):
    valid_substrings = [
        "https://www.adr.gov.co/",
        "https://www.alertabogota.com/",
        "https://www.alertatolima.com/",
        "https://thearchipielagopress.co/",
        "https://www.cali24horas.com/",
        "https://www.ccb.org.co/",
        "https://caracol.com.co/",
        "https://www.diariodelnorte.net/",
        "https://www.diariodelsur.com.co/",
        "https://diarioriente.com/",
        "https://elextra.co/",
        "https://www.elpaisvallenato.com/",
        "https://www.ecosdelcombeima.com/",
        "https://www.innovaspain.com/",
        "https://www.lafm.com.co/",
        "https://laguajirahoy.com/",
        "https://larazon.co/",
        "https://www.lavozdeyopal.co/",
        "https://www.las2orillas.co/",
        "https://www.llanoaldia.co/",
        "https://llanoalmundo.com/",
        "https://llanosietedias.com/",
        "https://www.noticiasdelmeta.com.co/",
        "https://noticierodelllano.com/",
        "https://periodicodelmeta.com/",
        "https://periodicovirtual.com/",
        "https://radio1040am.com/",
        "https://www.radionacional.co/",
        "https://radiosuper.com.co/",
        "https://www.radionica.rocks/",
        "https://revistaelcongreso.com/",
        "https://risaraldahoy.com/",
        "https://semanariolacalle.com/",
        "https://www.sinergiainformativa.com.co/",
        "https://tiemporeal.media/",
        "https://www.valoraanalitik.com/",
        "https://www.villavicenciodiaadia.com/",
        "https://villavoalreves.co/",
        "https://www.violetastereo.com/wp/",
        "https://www.viveelmeta.com/",
    ]

    def contains_valid_substring(url):
        return any(substring in url for substring in valid_substrings)

    filtered_data_frame = data_frame[data_frame["Cod_Url"].apply(contains_valid_substring)]
    return filtered_data_frame


@st.cache_data(ttl=3600)
def get_data():
    cursor = collection.find(query).limit(7000)
    df = pd.DataFrame(list(cursor))
    # Filter the DataFrame using the filter_urls function
    filtered_df = filter_urls(df)
    return filtered_df


# Get a batch of random news articles from the MongoDB collection
def get_random_news_batch(batch_size, df):
    available_indices = set(df.index) - served_articles
    if len(available_indices) < batch_size:
        # Reset served_articles if not enough articles are available
        served_articles.clear()
        available_indices = set(df.index)

    random_indices = random.sample(available_indices, batch_size)
    random_news_batch = df.loc[random_indices]
    served_articles.update(random_indices)  # Add the served article IDs to the set
    return random_news_batch


# Update the es_economica_manual field in MongoDB
def update_news_status(news_id, answer):
    to_update = collection.find_one({"_id": ObjectId(news_id)})
    if "es_economica_manual" not in to_update:
        collection.update_one({"_id": ObjectId(news_id)}, {"$set": {"es_economica_manual": answer}})


def process_updates():
    while True:
        news_id, answer = update_queue.get()
        update_news_status(news_id, answer)
        update_queue.task_done()


class NewArticle:
    def __init__(self, article, id, url, fecha, sim):
        st.write("Fecha de la noticia: " + fecha + "\n")
        st.write("Fuente: " + url + "\n")
        st.write("ID de la noticia: " + str(id) + "\n")
        st.write("Nivel de similaridad: " + str(sim) + "\n")

        # Remove specified symbols from the article string
        symbols_to_remove = "$|{}[]"
        translation_table = str.maketrans("", "", symbols_to_remove)
        cleaned_article = str(article).translate(translation_table)

        st.write(str(cleaned_article))
        # Display Yes and No buttons
        self.id = id
        self.yes_button = st.form_submit_button("Sí")
        self.no_button = st.form_submit_button("No")
        self.skip_button = st.form_submit_button("Omitir")  # Add skip button attribute


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

                news = NewArticle(article['Desc_Noticia'], article['_id'], article['Cod_Url'],
                                  str(article['Fecha_Noticia']), article['Max_similarity'])
                if news.yes_button or news.no_button:
                    update_queue.put((article["_id"], "Sí" if news.yes_button else "No"))
                    st.session_state.data.append({
                        '_id': article['_id'], 'es_economica_manual': "Sí" if news.yes_button else "No"})
                    st.session_state.num += 1
                    placeholder.empty()
                    placeholder2.empty()
                elif news.skip_button:  # Check if the skip button is clicked
                    st.session_state.num += 1
                    placeholder.empty()
                    placeholder2.empty()
                else:
                    st.stop()


st.title("Etiquetador de noticias")
st.write("Seleccione 'Sí', si la noticia es económica, de lo contrario seleccione 'No'.")
st.write(" Seleccione 'finalizar' para terminar la sesión")
main()
