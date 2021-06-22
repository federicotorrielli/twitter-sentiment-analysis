---
layout: cover
background: "https://w.wallhaven.cc/full/gj/wallhaven-gj9xp3.jpg"
---

# Twitter Sentiment Analisys

Progetto MAADB 2020/2021

Federico Torrielli && Ivan Spada

---
layout: center
---

# Goal del progetto

* **Natural Language Processing** di Twitter dataset
* **Performance Comparison** tra l'utilizzo di MySQL e MongoDB in un contesto reale
* Generazione di **definizioni** di dizionario/slang per word
* Generazione di risultati dell'analisi *per sentiment*
* Generazione di **WordCloud** per sentimento/emoji/emoticon

---

# Struttura del progetto

* 📕 Resources: Risorse offline (tweet, font, immagini, word-per-sentiment)
* 💻 src: Codice
    * 🖧 dao: Classi di connessione al DB
        * 🖳 dao: Sovraclasse di controllo
        * 🖳 dao_mongo_db: Persistenza per mongodb
        * 🖳 dao_mysql_db: Persistenza per mysql
    * 📎 datasets_manager: Interazione con i dati offline (Resources)
    * 📁 file_manager: Interazione low-lever con i files (open, close, append...)
    * 🌍 lexical_glob: Mantiene i nomi dei files offline utilizzati
    * 🌴 natural: Esegue il Natural Language Processing dei dataset
    * 👅 slang: Crea le definizioni per le word
    * 🚓 start: Inizializza tutto il programma
    * ⛅ wordcloudgenerator: Crea le WordClouds
    
---

# Le operazioni svolte

1. Si è iniziato pensando a come **analizzare le parole** e svogere *NLP* su di esse: utilizzo
   di **nltk** per tokenizzare e **Counter()** per calcolarne la frequenza
2. Dapprima il lavoro veniva fatto interamente offline, costruendo delle **funzioni dummy** che
   dessero la parvenza di persistenza, salvando il lavoro effettuato su file .toml
3. Si è dunque deciso di voler generare delle **definizioni** per le parole e per gli slang
   associando la word tokenizzata alla sua corretta definzione, ove possibile
4. Sempre con i dati offline, si sono generate le classi che avrebbero preso in input i dati da
   natural per poi generarne le **WordCloud**
5. Il lavoro è stato poi spostato a come poter comunicare i dati con i due diversi database
   con una sola classe di controllo, nonostante l'evidente diversità delle strutture.
6. Viene sviluppata la classe **Dao** e le sue specializzazioni (*MySQL* e *MongoDB*): essa viene costruita
   prima del NLP e fornisce input per natural e archivia l'output per l'utilizzo nelle WordCloud
7. Ogni dato viene ora fornito dai database: il tassello mancante è quello di immaginare una situazione
   di **produzione**, con server online (invece di *localhost*). Sono stati effettuati test con 
   diverse **piattaforme online**
   
---
layout: center
---

# Struttura tecnica del progetto

* **Ambiente**: Python 3.9
* **Moduli DAO utilizzati**: *pymongo* (MongoDB) e *pymysql* (MySQL)
* **Moduli NLP utilizzati**: *nltk*, nel caso specifico *TweetTokenizer*
* **Definizioni**: *Urban Dictionary* (slang) e *Dictionary API* (definizioni)
* **Riconoscimento Emoji/Emoticon**: *UNICODE_EMOJI_ENGLISH* dal modulo *emoji*

---
layout: center
---

# Database di testing/produzione utilizzati

### Testing:

* localhost
* VPS Contabo (location: Nurberg)

### Produzione:

* MongoDB Atlas Cluster (location: Francoforte)
* ScaleGrid Linode (location: Francoforte)

---

# Codice: Natural

* **Input**: (dal DAO) dizionario `{tweet_id: tweet}` e nome del sentiment (ad es. "anger")
* **Output**: dict `{token: count}`, dict `{hashtag: count}`, dict `{emoji: count}`, dict `{emoticon: count}`
  per ogni emozione, direttamente al DAO
* **Spiegazione**: per ogni dataset di tweet legato al sentimento andiamo a chiamare sotto-metodi per processare
  tweet-per-tweet e restituiamo quante volte quel token appare tra tutte le parole processate, pulendo
  i token dalle stopwords fornite. I dizionari in output verranno utilizzati dal dao per costruire le
  tabelle/document *sentiment_type_count*. Nella stessa classe creiamo anche le **definizioni** delle parole
  e i risultati finali che verranno poi trasferiti ai database rispettivi. Inoltre, creiamo le statistiche
  di uso delle risorse lessicali.
  
---

# I/O: Natural

* Per prendere i tweets dai db: `dao.get_tweets(...)`
* Per costruire i le tabelle/documenti di frequenza per sentimento: `dao.build_sentiments(...)`
* Per creare le definizioni: `slang.create_definitions(...)`
* Per prendere il count (appena messo) al fine di generare il risultato finale: `dao.get_counts(...)`
* Per prendere la definzione al fine di generare il risultato finale: `dao.get_definition(...)`
* Per prendere le percentuali di popularity-per-dataset al fine di generare il risultato finale: `dao.get_popularity(...)`
* Per caricare i risultati finali generati sui rispettivi database: `dao.push_results(...)`
* Per creare mettere l'index sulle word dei risultati finali, dopo l'upload: `dao.create_index(...)`
