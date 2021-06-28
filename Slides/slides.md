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
    * 📁 file_manager: Interazione low-level con i files (open, close, append...)
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

* MongoDB Atlas Cluster (location: Francoforte) Con autosharding abilitato
* ScaleGrid Linode (location: Francoforte)

---

# Codice: Natural

* **Input**: (dal DAO) dizionario `{tweet_id: tweet}` e nome del sentiment (ad es. "anger")
* **Output**: dict `{token: count}`, dict `{hashtag: count}`, dict `{emoji: count}`, dict `{emoticon: count}`
  per ogni emozione, direttamente al DAO
* **Spiegazione**: per ogni dataset di tweet legato al sentimento andiamo a chiamare sotto-metodi per processare
  tweet-per-tweet e restituiamo quante volte quel token appare tra tutte le parole processate, pulendo
  i token dalle **stopwords** fornite. I dizionari in output verranno utilizzati dal dao per costruire le
  tabelle/document *sentiment_type_count*. Nella stessa classe creiamo anche le **definizioni** delle parole
  e i risultati finali che verranno poi trasferiti ai database rispettivi. Inoltre, creiamo le **statistiche**
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

---

# Esempio: Natural

```python
def process_dataset(tweets: dict, sentiment: str):
    print(f"Started NLP for {sentiment}")
    start = timer()
    wordlist = []
    stopset = create_stopword_list()
    if '_id' in tweets:
        tweets.pop('_id')
    for tweet_id, phrase in tweets.items():
        processed_phrase = process_phrase(phrase, stopset)
        wordlist.append(processed_phrase)
        tweets_tokens[tweet_id] = processed_phrase  # used in relational db

    count_tuples = count_words(wordlist)
    most_used_hashtags = count_hashtags(count_tuples)
    emojis, emoticons = count_emojis(count_tuples)
    count_tuples = exclude_hastags_emojis(count_tuples)
    end = timer()
    print(f"Done processing in {end - start} seconds")
    return dict(count_tuples), dict(most_used_hashtags), dict(emojis), dict(emoticons)
```

---

# Codice: Slang

* **Input**: Una lista di dizionari di word
* **Output**: Le definizioni delle word in input, passate al DAO.
* **Spiegazione**: Per ogni parola viene cercata prima la sua definizione nel dizionario generale (DictionaryAPI).
  Se questa non viene trovata, si considera la parola uno slang, e quindi viene rifatta una ricerca nell'Urban Dicitonary.
  Se ancora non viene trovata, allora si tratta di un errore di scrittura oppure di un neologismo, e quindi viene ignorata.
  Per velocizzare la ricerca sui dataset, le definizioni vengono salvate in un file locale ".toml" in modo da poter essere
  recuperate se per qualche motivazione l'esecuzione del programma non andasse a buon fine.

---

# I/O: Slang

* Per caricare le definizioni delle parole: `dao.dump_definitions(...)`
* Per caricare in locale le definizioni pre-esistenti: `file_manager.dump_toml(...)`
* Per scaricare i JSON delle definizioni delle API: `requests.get(...)`

Nella slide seguente andiamo ad includere un pezzo del codice per recuperare il JSON della definizione dalla API Slang

---

# Esempio: Slang

```python
def get_slang_definition(word):
    response = get(f"https://api.urbandictionary.com/v0/define?term={word}")
    response.encoding = 'utf-8'
    good_definition = {"definition": "", "count": 0}
    if response.ok:
        text = response.json()
        if 'list' in text:
            for elem in text['list']:
                if elem['thumbs_up'] > elem['thumbs_down'] and good_definition['count'] < elem['thumbs_up']:
                    good_definition = {"definition": elem['definition'], "count": elem['thumbs_up']}
    return good_definition["definition"]
```
Possiamo osservare come, essendo Urban Dictionary un dizionario di comunità, debba essere selezionata la
"risorsa migliore" tra quelle esistenti, andando a selezionare solo quella con like > dislike e con
like molto maggiori tra tutte le definizioni recuperate.

---

# Codice: Dao

* **Input**: Il tipo di DB che si intende utilizzare
* **Output**: Nessuno.
* **Spiegazione**: Da questa classe vengono chiamati i metodi per entrambi i database in egual modo,
  comportandosi effettivamente come "sovraclasse" per dao_mongo e dao_mysql.
  
---

# I/O: Dao

* Chiamata ai metodi della classe dao_mongo per le operazioni da/a MongoDB
* Chiamata ai metodi della classe dao_mysql per le operazioni da/a MySQL

---

# Esempio: Dao

```python
    def get_definition(self, word: str, sentiment: str = "") -> str:
        """
        Given a word it returns the correct definition, given that
        it is already stored on the database
        @param word: the word to search the definition for
        @param sentiment: (optional) the sentiment to search it for
        @return: the definition of the word
        """
        if self.type_db:
            return self.dao_type.get_definition(word.lower())
        else:
            return self.dao_type.get_definition(word.lower(), sentiment.lower())
```
Si può notare come molte delle chiamate ai metodi dei due database, nonostante si tratti
delle stesse risorse, debbano essere trattate in maniera totalmente diversa, a seconda
delle differenti situazioni.

---

# Codice: dao_mongo_db (1)

* Gestisce **tutte le operazioni** da e verso MongoDB
* Inizializza e popola il DB delle informazioni necessarie, se richiesto

```python
    def build_db(self, sentiments, words, emoticons, emojis, twitter_paths):
        print("Dropping the previous database...")
        self.database.command('dropDatabase')

        print("Inserting the tweets on the database...")
        for index, sentiment in enumerate(sentiments):
            tweet_document = self.database[f'{sentiment}_tweets']
            self.__insert_tweets(tweet_document, twitter_paths[index], sentiment)
            word_document = self.database[f'{sentiment}_words']
            self.__insert_words(word_document, words, index)
            emoji_document = self.database[f'{sentiment}_emoji']
            emoji_document.insert(emojis)
            emoticon_document = self.database[f'{sentiment}_emoticons']
            emoticon_document.insert(emoticons)
```
* Ogni singola operazione è stata divisa in sotto-operazioni convenienti per rendere
  leggibile il proprio codice (come `__insert_tweets()` oppure `__insert_words()`)
  
---

# Codice: dao_mongo_db (2)

<img src="https://i.imgur.com/Z3mbj8x.png" class="h-80 rounded shadow">

Alcuni dei metodi utilizzati per comunicare in maniera conveniente con mongo_db

---

# dao_mongo_db: Vantaggi nel progetto

* **Elasticità** nella definizione di come una struttura deve essere utilizzata o creata
* Non esiste affatto uno schema a cui bisogna "aderire fedelmente", tutti i dati sono BSON o **JSON**,
  convertiti a dizionari Python, nel nostro caso specifico
* Buon supporto per linguaggi di programmazione ad alto livello, ad esempio Python, per le operazioni
  fondamentali
* Setup per la produzione facile
* Utilizzo di **secondary index** (anche nested) per velocizzare una query di ricerca (vd. nostro utilizzo
  in results)
* Scalabilità assoluta che si adatta facilmente a progetti in continua evoluzione
* Aiuta nella scrittura di codice pulito, veloce e leggibile
* Velocità insuperabile di data deposit e retrieval
* Facile da esplorare con le find()

---

# dao_mongo_db: Svantaggi nel progetto

* Proprio il fatto di essere **schema-less** rende difficile la definizione di una **struttura generale** di come deve essere
  trattato il DB, come ogni oggetto deve essere richiesto ed inviato, e alle volte si crea del *disordine* quando
  non si riesce a dare una forma alla struttura generale dei dati
* Ci si ritrova facilmente in situazioni di ridondanza di dati in diverse tabelle
* Alto **utilizzo di memoria**, soprattutto per quanto riguarda indici
* Necessità di utilizzare alle volte dei **trucchetti** per far "entrare" delle strutture dati in JSON

---

# dao_mongo_db: Performance in produzione

* MongoDB completa la **build dei componenti base del database** (lista di tweet, words, emoji, emoticon da utilizzare)
  in **23 secondi** su una connessione 5MBps in download e 3MBps in upload, dunque il vero ostacolo non è tanto il database
  quanto invece la velocità di connessione.
* MongoDB completa l'intero processo (Build, Natural Language Processing, Creazione new lexicon, Creazione results, Wordclouds)
  in **120 secondi circa** su un Replica Set da 3 nodi.
* MongoDB, a fine elaborazione dei file di progetto, ha un'occupazione in memoria di **94MB** circa, di cui 22 per i results

---

# dao_mysql_db (1)

* Gestisce tutte le operazioni da e verso MySQL
* Inizializza e popola il DB delle informazioni necessarie, se richiesto

```python
    def build_db(self, sentiments, words, emoticons, emojis, twitter_paths):
        print("Building DB")
        self.__drop_and_create_tables()
        print("\n\tAdding sentiments...")
        self.__insert_sentiments(sentiments)
        print("\n\tAdding emoticons...")
        self.__insert_emoticons(emoticons)
        print("\n\tAdding emojis...")
        self.__insert_emojis(emojis)
        print("\n\tAdding words...")
        self.__insert_words_sentiments(words)

        print("\n\tAdding tweets...")
        assert len(sentiments) == len(twitter_paths)
        tweets_content = {sentiment: [] for sentiment in sentiments}
        for idx_sentiment, file_path in enumerate(twitter_paths):
            tweets_content[sentiments[idx_sentiment]] = read_file(file_path).splitlines()

        self.__insert_tweets_sentiments(tweets_content)
```

---

# dao_mysql_db (2)

<img src="https://i.imgur.com/iEMpSnc.png" class="h-100 rounded shadow">

---

# dao_mysql_db: Schema ER (1)

<img src="http://evilscript.altervista.org/files/img/er2.png" class="h-100 rounded">

---

# dao_mysql_db: (2)

<img src="http://evilscript.altervista.org/files/img/er1.png" class="h-100 rounded">

---

# dao_mysql_db: Vantaggi nel progetto

* Schema ordinato che tende a mantenersi nel tempo
* I dati sono consistenti a priori
* Linguaggio **SQL** per interagire con il database: semplifica l'interazione con i dati utilizzando sfruttando una sintassi semplice
* Flessibilità nel cambio di requisiti dell'applicazione: SQL facilita la modifiche delle interrogazioni sui dati 
* L'operatore **JOIN** semplifica la realizzazione delle interrogazioni sui dati presenti su più tabelle sfruttando le chiavi
* L'utilizzo di chiavi permette il check dei dati in ingresso (**PK/FK**, **CHECK** dei valori definendo i vincoli) e l'esecuzione di aggiornamenti in cascata grazie ai vincoli di chiave (**ON DELETE/ON UPDATE**)
* L'operatore **UNIQUE** permette il controllo dei duplicati dei dati in ingresso

---

# dao_mysql_db: Svantaggi nel progetto

* Schema molto **rigido**, poco flessibile alle modifiche strutturali, è da stabilire con particolare precisione a inizio progetto
* Non è possibile avere attributi con valori annidati e quindi bisogna operare su più tabelle utilizzando i JOIN
* Le interrogazioni che coinvolgono più tabelle richiedono l'utilizzo dell'operatore JOIN che risulta essere "**pesante**" e lento
* **Impedance mismatch**: è richiesta una continua "traduzione" delle strutture dati, MySQL ha una struttura **tabellare** mentre l'applicazione tende ad utilizzare strutture dati più avanzate che vengono scelte in funzione delle necessità applicative (ad esempio, nel codice è frequente l'utilizzo di **dict**)
* Non è adatto a lavorare in maniera distribuita poiché strettamente legato alle proprietà ACID
* Non essendo un **replica set**, è necessario prevedere un sistema di backup dei dati che permetta il recupero in caso di guasto


---

# dao_mysql_db: Performance in produzione

* MySQL completa la **build dei componenti del database** (lista di tweet, sentimenti, words, emoji, emoticon da utilizzare)
  in **59 secondi** su una connessione 5MBps in download e 3MBps in upload e **30 secondi circa** su localhost.
* MySQL completa l'intero processo (Build, Natural Language Processing, Creazione new lexicon, Creazione results, Wordclouds)
  in **1 ora circa** (**262 secondi** su localhost).
* MySQL, a fine elaborazione dei file di progetto, ha un'occupazione in memoria di **70MB** circa

---
layout: two-cols
---

# MongoDB

* Base di dati documentale

* NoSQL

* Schema-less

* Organizzazione dei dati flessibile

* Interazione con i dati veloce

* All'aumentare dei dati, si mantengono le prestazioni

* Strutture dati vicine al codice

* Lavora bene su cluster di macchine

::right::

# MySQL

* Modello relazionale

* SQL

* Schema dei dati rigido

* Dati organizzati in tabelle

* Interazione con i dati più lento

* All'aumentare dei dati, le prestazioni peggiorano

* Impedance mismatch

* Non adatto a una soluzione distribuita 

---
layout: two-cols
---

# Results: Standard

```python
{'_id': ObjectId('60d6f42d533910561a713fd9'),
 'count': {'anger': 94,
           'anticipation': 53,
           'disgust': 50,
           'fear': 42,
           'joy': 17,
           'sadness': 73,
           'surprise': 76,
           'trust': 11},
 'def_type': 'standard',
 'definition': 'An electronic device for storing and processing data, '
               'typically in binary form, according to instructions given to '
               'it in a variable program.',
 'popularity': {'anger': 0.0606,
                'anticipation': 0.0548,
                'disgust': 0.0329,
                'fear': 0.027,
                'joy': 0.004,
                'sadness': 0.0578,
                'surprise': 0.1364,
                'trust': 0.0089},
 'typo': 'no',
 'word': 'computer'}
```

::right::

# Results: Slang

```python
{'_id': ObjectId('60d6f42d533910561a706db8'),
 'count': {'anger': 30,
           'anticipation': 29,
           'disgust': 28,
           'fear': 19,
           'joy': 13,
           'sadness': 35,
           'surprise': 18,
           'trust': 6},
 'def_type': 'slang',
 'definition': '[Short] for "should have".',
 'popularity': {'anger': 0.0193,
                'anticipation': 0.03,
                'disgust': 0.0184,
                'fear': 0.0122,
                'joy': 0.0031,
                'sadness': 0.0277,
                'surprise': 0.0323,
                'trust': 0.0049},
 'typo': 'no',
 'word': 'shoulda'}
```

---
layout: two-cols
---

# Results: Typo

```python
{'_id': ObjectId('60d6f42d533910561a702d03'),
 'count': {'anger': 259,
           'anticipation': 170,
           'disgust': 284,
           'fear': 144,
           'joy': 287,
           'sadness': 157,
           'surprise': 300,
           'trust': 188},
 'def_type': 'standard',
 'definition': 'NOTHING FOUND',
 'popularity': {'anger': 0.1669,
                'anticipation': 0.1756,
                'disgust': 0.187,
                'fear': 0.0925,
                'joy': 0.0676,
                'sadness': 0.1242,
                'surprise': 0.5386,
                'trust': 0.1526},
 'typo': 'yes',
 'word': 'yo'}
```

::right::

# Spiegazione

Il neologismo "yo", nonostante sia utilizzato tantissimo tra i tweet trovati
si configura come un **typo** della parola *"you"*, spesso usato come abbreviazione.

Per questa motivazione non viene trovata alcun tipo di definizione in entrambi i
dizionari, e viene perciò catalogato come *typo* dal sistema!

---
layout: two-cols
---

# Distribuzione delle emozioni di Plutchik nel corpus

<img src="http://evilscript.altervista.org/files/img/barchart.png" class="h-70 rounded">

::right::

# Percentuali

`(perc_presence_lex_res, perc_presence_twitter)`

```python
{'anger': [0.38497652582159625, 0.024783147459727387],
 'anticipation': [0.5272727272727272, 0.017438364401683705],
 'disgust': [0.46324891908585547, 0.022746572849690647],
 'fear': [0.4219228413962033, 0.023552334723456622],
 'joy': [0.26741110784601824, 0.07440516747976343],
 'sadness': [0.4308390022675737, 0.018962075848303395],
 'surprise': [0.5454545454545454, 0.008794872531831116],
 'trust': [0.4939073923639318, 0.01806405609364787]}
```

---

# Calcolo delle percentuali (Natural)

```python
def calc_perc_sharedwords(shared_words, word_datasets):
    """
    Return a set of tuples (perc_presence_lex_res, perc_presence_twitter)
    @param shared_words: dict of list af words foreach Plutchik emotion
    @param word_datasets: datasets of phrases
    @return: set of tuples (perc_presence_lex_res, perc_presence_twitter)
    """
    lex_words = get_lexical_Nlines()
    returndict = {}
    index = 0
    sentiments = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]
    for wordlist in shared_words.values():
        n_shared_words = len(wordlist)
        n_twitter_words = len(word_datasets[index])
        print(f"Dataset: {index}, #Shared: {n_shared_words}, #Twitter: {n_twitter_words}, #Lex: {lex_words[index]}")
        perc_presence_lex_res = n_shared_words / lex_words[index]
        perc_presence_twitter = n_shared_words / n_twitter_words
        returndict[sentiments[index]] = (perc_presence_lex_res, perc_presence_twitter)
        index += 1
    return returndict
```

---

# WordClouds (Anger)

<div>
  <img src="https://evilscript.altervista.org/files/img/anger_plot.png" class="h-60 rounded" style="margin: 0 auto">
</div>
<div grid="~ cols-2 gap-4">
  <img src="https://evilscript.altervista.org/files/img/anger_emoticon_plot.png" class="h-50 rounded">
  <img src="https://evilscript.altervista.org/files/img/anger_emoji_plot.png" class="h-50 rounded">
</div>

---

# WordClouds (Anticipation)

<div>
  <img src="https://evilscript.altervista.org/files/img/anticipation_plot.png" class="h-60 rounded" style="margin: 0 auto">
</div>
<div grid="~ cols-2 gap-4">
  <img src="https://evilscript.altervista.org/files/img/anticipation_emoticon_plot.png" class="h-50 rounded">
  <img src="https://evilscript.altervista.org/files/img/anticipation_emoji_plot.png" class="h-50 rounded">
</div>

---

# WordClouds (Disgust)

<div>
  <img src="https://evilscript.altervista.org/files/img/disgust_plot.png" class="h-60 rounded" style="margin: 0 auto">
</div>
<div grid="~ cols-2 gap-4">
  <img src="https://evilscript.altervista.org/files/img/disgust_emoticon_plot.png" class="h-50 rounded">
  <img src="https://evilscript.altervista.org/files/img/disgust_emoji_plot.png" class="h-50 rounded">
</div>

---

# WordClouds (Fear)

<div>
  <img src="https://evilscript.altervista.org/files/img/fear_plot.png" class="h-60 rounded" style="margin: 0 auto">
</div>
<div grid="~ cols-2 gap-4">
  <img src="https://evilscript.altervista.org/files/img/fear_emoticon_plot.png" class="h-50 rounded">
  <img src="https://evilscript.altervista.org/files/img/fear_emoji_plot.png" class="h-50 rounded">
</div>

---

# WordClouds (Joy)

<div>
  <img src="https://evilscript.altervista.org/files/img/joy_plot.png" class="h-60 rounded" style="margin: 0 auto">
</div>
<div grid="~ cols-2 gap-4">
  <img src="https://evilscript.altervista.org/files/img/joy_emoticon_plot.png" class="h-50 rounded">
  <img src="https://evilscript.altervista.org/files/img/joy_emoji_plot.png" class="h-50 rounded">
</div>

---

# WordClouds (Sadness)

<div>
  <img src="https://evilscript.altervista.org/files/img/sadness_plot.png" class="h-60 rounded" style="margin: 0 auto">
</div>
<div grid="~ cols-2 gap-4">
  <img src="https://evilscript.altervista.org/files/img/sadness_emoticon_plot.png" class="h-50 rounded">
  <img src="https://evilscript.altervista.org/files/img/sadness_emoji_plot.png" class="h-50 rounded">
</div>

---

# WordClouds (Surprise)

<div>
  <img src="https://evilscript.altervista.org/files/img/surprise_plot.png" class="h-60 rounded" style="margin: 0 auto">
</div>
<div grid="~ cols-2 gap-4">
  <img src="https://evilscript.altervista.org/files/img/surprise_emoticon_plot.png" class="h-50 rounded">
  <img src="https://evilscript.altervista.org/files/img/surprise_emoji_plot.png" class="h-50 rounded">
</div>

---

# WordClouds (Trust)

<div>
  <img src="https://evilscript.altervista.org/files/img/trust_plot.png" class="h-60 rounded" style="margin: 0 auto">
</div>
<div grid="~ cols-2 gap-4">
  <img src="https://evilscript.altervista.org/files/img/trust_emoticon_plot.png" class="h-50 rounded">
  <img src="https://evilscript.altervista.org/files/img/trust_emoji_plot.png" class="h-50 rounded">
</div>
