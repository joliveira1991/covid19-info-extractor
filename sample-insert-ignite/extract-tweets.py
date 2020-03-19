# Basic Imports
import tweepy
from tweepy import OAuthHandler
import pandas as pd
import numpy as np
import time
import os
import re

# import google translate
from googletrans import Translator

# import ignite
from decimal import Decimal
from pyignite import Client

# TextBlob Imports
from textblob import TextBlob

# NLTK Imports
#import nltk
#from sklearn.feature_extraction.text import CountVectorizer
#from sklearn.naive_bayes import MultinomialNB

TWEET_TABLE_NAME = 'tweet_history'

TWEET_CREATE_TABLE_QUERY = '''CREATE TABLE if not exists tweet_history (
    epoch bigint,
    external_id bigint,
    tweet varchar(4000),
    length integer,
    date varchar(500),
    source varchar(500),
    count_likes integer,
    count_re_tweets integer,
    location varchar(500),
    geo varchar(4000),
    coordinates varchar(4000),
    positive_words_count integer,
    negative_words_count integer,
    neutral_words_count integer,
    polarity varchar(500),
    primary key (external_id)
)'''

DATASET_POSITIVO=['sim','bom','positiva','positivo','abnegação','abrace','abrigo','absoluto','absolutamente','absorvido','abundam','abundante','abundância','acaronar','acclaim','aceita','aceitação','aceitável','aceito','aceitando','acelerando','acessível','achievable','aclamado','aclamação','acomodar','acomodado','acomodação','acomodando','acomodatícia','aconchego','actability','activo','habilitado','adaptabilidade','adaptáveis','adaptável','adequada','adição','admirado','admiravelmente','admiração','admire','admirável','adorado','adorando','adoração','adore','adorável','advanced','afabilidade','afavelmente','affluent','afirmativo','afirmação','afirme','afluência','afável','agilidade','agradado','agradavelmente','agradecemos','grata','agradecimento','agradável','ainda','alcançar','alegre','alegre','alegria','alegria','alegre','alegria','alerta','alimentando','consolide','alinhados tudo vai bem','alive','vivacidade','aliviar','aliviado','aloha','altamente distinta','altamente distinto','altitudinarian','altivo','altrucause','altruisticamente','altruístas','alucinante','além de','além fabuloso','alívio','amabilidade','amada','amando aceitação','amante da beleza','ambição','amiabily','amigo','amigo','amizade','uso','amigável','amin','ampla','amável','angélico','animada','animado','animado antecipação','animação','ansiar','ansioso','ânsia','antecipação','apenas','aplaudir','apoio','apoio','suportado','aprecia','apreciativo alegria','apreciação da beleza','apreciáveis apreciado','aprender','learning','aprovar','aprovação','ardente','ardor','ardência','art de quietude','arte de agradecimento','arte de bem-estar','assertividade','assiduidade','assunção de riscos','astronômico','atencioso','consideração','atenção','aterrada','atitude positiva palavras','ativar','atos de bondade','atraente','atrativo','atração','audácia','auto-bondade','auto-estima','auto-expressão','auto-perdão','autorizando palavras','autêntico','autenticidade','aventura','ação','ação para a felicidade','balance','equilibrado','balistic de desmonte','bastante','beatificar','beatitude','be happy','beijo','beingness','beleza','bonito','lindamente','beleza em todas as coisas','bem-estar','bem-humorada','bem-vindo','bem estar','benefício','benefícios','benevolente','benevolentemente alegre of mind','benfeitor','benéfico','biophilia','bless','bênção','abençoado','bliss','blissfulness','feliz','blisscipline','bliss on tap','boa espírito residente','boa palavra','boa saúde','bom','bom humor','bondade','espécie','tipo-coração','gentilmente','bondade amorosa','bonito','bons sentimentos','borbulhando','breathtaking','brilhante','brilhante cores','brilhe','brilho','brilliance','brilhante','brio','buyancy','calma','calmo','tranquilidade','campeão ‘','capaz','capacidade','competentemente','capital','carinho','carinhoso','carisma','carismática','caritativo','caridade','carícia','catita','cavalheiro','ceder','cegando','centrado','certos','certeza','chakra','charme','encanto','charmer','circunstâncias positivas','clarity','claro','claro dirigido','clean','limpeza','colaboração','com apreço','comemorar','celebração','compaixão','compassivo','companheirismo','competente','competência','competency','competência','com ternura','comunhão','comunicação','comunidade','concentração','concorda','concurso','confiante','confiável','confiabilidade','confiável','confiança','conforto','confortável','consolação','congruência','conhecimento','connect','conectado','conexão','conexidade','conquistar','consciente','mindfulness','consciência','consistência','consistente','contente','conteúdo','contentamento','continuidade','continuando','contribuição','convicção','convincentes','cooperação','coragem','corajosa','coração','coração de abertura','cordial','cordialidade','cortesia','cortês curioso','curiosidade','crank (up)','crenças positivas','crescer','crescimento','crescimento pessoal','criatividade','criativo','criatividade','criatividade','crie','crível','cuddle','carinho','cuidado','cuidados','importando-se','cumprir','cumpridas','cura alternativa','cuteness','cuteness incredible','céu','celestial','dandy','dauwtrappen','decente','dedicado','dedicação','de forma amigável','de fácil abordagem','de fácil utilização','delicado','deliciosa','delicioso','delícia','de luxo','de mente aberta','de renome','de repouso','de saber','desafio','descoberta','desejo','desejável','desenvoltura','de ser conhecido','deslumbrado','destacamento','determinação','deus','deusa','dever','devotado','dignidade','diligente','direção','disciplina','discrição','disney','dispostos disposto','vontade','diversidade','divertido','divino','do','dream','sonhador','drive','dynamic','dê','dar','ecosofia','ecstatify','educado','educar','educação','educado','efervescência','eficiente','eficiência','eficácia','elation','elegance','elevate','elevado','elogios','elétrico','em-amor','embelezar','em diante','emocionado','emocionante','emoção','animado','emoções positivas','empatia','empatia','enfático','empower','responsabilizar','autorizado','em transe','emular','encantado','endurance','energia positiva','energética','energize','energia','enorme','enormous','entende','entendimento','entendida','entusiasmado','entusiasmo','envolver','envolvidos','equanimidade','equidade','eqüitativa','equitable','equipe','equânime','escolha','espanta','espanto','espaço','espaçosos','especial','esperança','esperança','espetacular','espírito','espírito positivo','estabilidade','estado benevolentemente','estagnação','estando em repouso','estilo','estimulado','estimular','estímulo','estudo','estudioso','estupendo','eterno','eudaemonist','eudaemonistic','eudaimonia','eudamonia','eudemonismo','eufórico','eventos positivos','evolui','exactidão','exaltado','exaltando','exaltação','excelência','excelente','excepcional','excessivamente otimista','excitado','exemplares','expansivo','expectante','experiência','exploração','expressividade','expressando','expressão sexual','exstatisfy','extra','extraordinário','extrovertido','exuberante','exuberance','exultante','fabuloso','facilidade','fair','fame','famoso','família','fantabulous','fantasia','fantastic','fascinate','fascinada','favorito','fazendo uma diferença','façanha','feito-natureza','felicidade','feliz','felizmente','feliz hearted','festivo','festiveness','fiabilidade','fidelidade','fiel','filhotes de cachorro','fina','firmeza','fit','flawless','flexibilidade','flor','floreio','flutuabilidade','foco','fondle','for caso disso','freecycle','frugalidade','ftw','funerific','funology','futuro','fácil','facilmente','mais fácil','fácil de falar','fé','ganhou','gatinhos','gemutlichkeit','generavity','generoso','generosidade','gengibre','genial','genius','gentileza','genuíno','gerar','gibigiana','gigantesco','gigil','glad','glamour','glory','glow','gosto','gostoso','grace','grande','grandiosidade','gratidão','gratefulness','grato','obrigado','obrigado','agradecimentos','greatful','groovy','guia','orientadores','orientação','guiar','habilidade','hábil','habitação','halo','harmonioso','harmonizar','harmonia','heartfelt','help','útil','ajudando','herói','heroísmo','high-spiritedness','honra','hospitaleiro','hospitaleiro','hospitalidade','hot','honesto','honestidade','humano','humilde','humor','idea','idealismo','igualdade','ikigai','iluminação','iluminação','iluminado','ilustre inner','ilustre inner','imaginação','impagável','imperturbável','impressionante','inacreditável','incentive','incentivo','incentivada','inclusão','incomparável','incondicional','incrível','independência','indo para o extra mile','inefável','infinidade','infinito','influência','iniciar','inocente','inovar','inovação','inquisitive','insight','insightfulness','perspicaz','inspiração palavra','inspire','inspiração','inspirado','inspirado','insuportavelmente bonito','integridade','inteiro','inteligência','inteligente','intenção','interessante','juros','interessado','intervir','envolvido','intrepid','intrigado','intuição','invencível','investimento','iridescent','irmãos de sangue','irreal','jovial','jubilante','justiça','juvenescent','keen','leader','liderança','lealdade','fiel','legal','liberdade','lindo','gorgeousness','logic','longevidade','love','adorável','amando','luto','luz','luz de nevoeiro','luz do sol','maduro','magnífico','mais','majestade','major','mantenha-up','maravilha','maravilhoso','mastery','maturidade','meditação','melhor','melhoria','melhoria das qualificações','meliorismo','memorável','merit','milagre','misericórdia','modéstia','morphing','motive','movido','movimento','mudança','mudanças','muito','muitos','mutualidade','mágica','namaste','neotenia','nirvana','nobre','notável','novo','nutrir','nutrido','nutritivo','alimentação','não-resistência','não resistente','o amor-próprio','obediente','o comprometimento','ok','olá','o melhor de todos os mundos possíveis','on','open','abertamente','abertura','o poder','poderoso','oportunidade','optimista','otimista','ordem','o reconhecimento','organização','orgulho','orgulhoso','orientação','original','originalidade','ostentando','otimismo','ousadia','ousado','paciência','pagar','paixão','apaixonado','paradise','paradisíacas','para rir','rir','para ser','para ser visto','parentesco','passos activa e construtiva','paz','pacificar','paz de espírito','paz interior','pendentes','pensamento positivo','pensamentos positivos','pep','peppiness','perdoe','perdoando','perdão','perdão','perfeito','perfeição','perkiness','perseverança','persistência','perspectivas','pertence','pertencentes','piada','piadas engraçadas','pias','picante','pick-me -up','play','brincalhão','ludicidade','pontualidade','pontual','por favor','por uma razão de ser','positivo','possibilitado','poupança','power-on','power-up','poço','praticidade','prazer','prazer','encantado','delicioso','prazer ousado','precisamente','preciso','precisão','presente','presença','preservação','presteza','privacidade','proatividade','progresso','prontidão','pronto','prontidão','prosperidade humana','prosperidade próspero','proteger','proto','pure','puro','qualidade','querido','quiddity','radiante','rainbow','rainha','rasasvada','razão','real','a realidade','realizado','realização','realizar','realizações','rebentar','recolhida','recomendar','reconfortante','recurso','redigido','refrescado','rejuvenescer','rejuvenescido','relax','relaxado','relações','religião','renascido','renomado','renovar','renovada','repose','repouso mente','reputação','reputação','requintado','resilient','resilience','resistência','resoluto','feistiness','respeitada','respeito','responsabilidade','rest','descansado','restaurar','restaurado','resultado','retidão','revelação','reverência','revigorar','revigorado','revived','rindo','ripe','riqueza','rolhagem','romance','romântico','rosiness','sabedoria','safe','segurança','sagrado','santa','santidade','satisfação abundante','satisfeito','save','savour','saboreando','saúde','saudável','sedutor','sedutora','se esforçam','seguro','seguro','segurança','seguros','sem esforço','sem medo','sentimentos amorosos','sentimentos positivos','sentimentos positivos vocabulário positivo','serendipity','sereno','serenidade','ser extraordinário','serviço','se sentir bem','sentir-se bem','sexy','sexiness','shape -shifting virtuoso','-stretching alma','shine','shining','significado','significativa','sim','simples','simplificar','simplicidade','simpático','sinceridade','sinergia','sistematização','sleep','smile','sorriso','sorte','lucky','soulmate','spark','faísca','sparkles','spellbound','splendid','suave','sublime','suficiente','sunniness','superado','superpotência','supreme suculenta','surpreenda','surpreendentemente','surpreendido','surpresa','sustentar','sustentado','sweet','doçura','sábio','sério','tato','teach','dócil','tempo','tempo optimista','tenacidade','terapia','thrive','próspera','tidsoptimist','tolerância','touch','tocado','trabalhadores da luz','trabalho','trabalho em equipe','tradição','tranquilo','tranquilidade','transformador','transformação','transformar','transparente','triunfo','true','unidade','unificação','unificação de mente','unique','up','upgrade','valor','valores','valioso','vantagem','vantagens','vantajosa','vantajosamente','variedade','verdade','veracidade','verificar a','versatilidade','vertiginoso','vibrante','vida','vida do partido','vidas através','vigor','vim','virtude','virtuosa','vitalidade','vitória','vitorioso','vivacidade','vivacidade','vivo','vida','viável','você é amado','vontade alegre','voto','vulnerabilidade','vulnerável','válido','ágil','água','âmbito','êxtase','arrebatador','útil']


DATASET_NEGATIVO=['mau','horror','lixo','nojo','violência','guerra','chatice','abaixo','pior','erro','crime','óbito','morte','chorar','lágrima','choro','castigo','morreu','óbito','funeral','morta','morto','péssimo','negativo','negativa','chato','salta fora','sai','sair','mal','negro','escuro','frustrante','desmotivador','desmotivante','contra-produção','contra-produtivo','perder','derrota','difícil','impossível','acabar', 'acabou','terminar','fechar','terminado','fechado','crontrair','contraiu','doença','mau estado','prejuízo','fechem','esquecer','esqueça','dificuldade','despesa','custo','perda']

class TwitterClient(object):
    '''
    Generic Twitter Class for sentiment analysis.
    '''
    def __init__(self):
        '''
        Class constructor or initialization method.
        '''
        # keys and tokens from the Twitter Dev Console
        consumer_key = 'XXXXXXXXXX'
        consumer_secret = 'XXXXXXXXXX'
        access_token = 'XXXXXXXXXXXXXXXXXXX'
        access_token_secret = 'XXXXXXXXXXXXXXXXXXX'

        self.translator = Translator()

        # attempt authentication
        try:
            # create OAuthHandler object
            self.auth = OAuthHandler(consumer_key, consumer_secret)
            # set access token and secret
            self.auth.set_access_token(access_token, access_token_secret)
            # create tweepy API object to fetch tweets
            self.api = tweepy.API(self.auth)
        except:
            print("Error: Authentication Failed")

        # attempt to connect ignite
        try:
            # establish connection
            self.client = Client()
            self.client.connect('127.0.0.1', 10800)

            # create tables
            for query in [
                TWEET_CREATE_TABLE_QUERY,
            ]:
                self.client.sql(query) 

        except Exception as ex:
            print("Error: Error on connection to ignite")
            print(ex)


    def get_tweets(self, query):
        '''
        Main function to fetch tweets and parse them.
        '''
        # empty list to store parsed tweets
        tweets = []

        try:
            # call twitter api to fetch tweets
            fetched_tweets = self.api.search(q = query, tweet_mode='extended', count = 1000,result_type = "recent", include_entities = True, lang = "pt")

            return fetched_tweets

        except tweepy.TweepError as e:
            # print error (if any)
            print("Error : " + str(e))

    def search_tweets_correct_ignite(self,query):
        #Searchin Twitter Timelines
        tweets = []
        info = []

        #get last tweets
        new_tweets = self.get_tweets(query = query)

        # for each tweet, check the sentiment analysis
        for tweet in new_tweets:
            #byte_count = ''.join(format(ord(i), 'b') for i in tweet)

            emoji_pattern = re.compile("["
                u"\U0001F600-\U0001F64F"  # emoticons
                u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                u"\U0001F680-\U0001F6FF"  # transport & map symbols
                u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                u"\U00002702-\U000027B0"
                u"\U000024C2-\U0001F251"
                u"\U0001f926-\U0001f937"
                u'\U00010000-\U0010ffff'
                u"\u200d"
                u"\u2640-\u2642"
                u"\u2600-\u2B55"
                u"\u23cf"
                u"\u23e9"
                u"\u231a"
                u"\u3030"
                u"\ufe0f"
                "]+", flags=re.UNICODE)

            is_a_retweet = 0
            
            try:
              tweet.retweeted_status.full_text
            except Exception as ex2:
              var_exists = False
            else:
              tweet.full_text = tweet.retweeted_status.full_text
              is_a_retweet += 1

            without_emojis = emoji_pattern.sub(r'',tweet.full_text)
            cleaned_tweet = ''.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])(\w+:\/\/\S+)", " ", without_emojis)).replace('\n',' - ').encode('utf-8', 'ignore').decode('utf-8')
            
            total_words_count = len(cleaned_tweet.split())
            positive_words_count = 0
            negative_words_count = 0
            neutral_words_count = 0

            # check if is positive
            for word in cleaned_tweet.split():
                if any(word.lower() in str(s).lower() for s in DATASET_POSITIVO):
                   positive_words_count += 1

            # check if is negative
            for word_n in cleaned_tweet.split():
                if any(word_n.lower() in str(s).lower() for s in DATASET_NEGATIVO):
                   negative_words_count += 1

            # check neutral
            neutral_words_count = total_words_count - positive_words_count - negative_words_count

            # check if tweet was already processed

            # translate expression to english
            english_expression = self.translator.translate(cleaned_tweet,dest='en', src='pt')
            analysis = TextBlob(english_expression.text)

            # check sentiment polarity
            if analysis.sentiment.polarity > 0:
               polarity = 'positive'
            elif analysis.sentiment.polarity == 0:
               polarity = 'neutral'
            else:
               polarity = 'negative'

            # if not exists, insert
            insert_statment = 'INSERT INTO tweet_history (epoch,external_id,tweet,length,date,source,count_likes,count_re_tweets,location,geo,coordinates,positive_words_count,negative_words_count,neutral_words_count,polarity) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
            row_epoch = int(round(time.time() * 1000))
            row = [row_epoch,tweet.id,str(cleaned_tweet),len(cleaned_tweet),str(tweet.created_at),str(tweet.source),tweet.favorite_count,tweet.retweet_count,str(tweet.user.location),str(tweet.geo),str(tweet.coordinates),positive_words_count,negative_words_count,neutral_words_count,str(polarity)]

            try:
               self.client.sql(insert_statment,query_args=row)
            except Exception as ex1:
               print(ex1)

def main():
    # creating object of TwitterClient Class
    api = TwitterClient()
    # calling function to get tweets
    api.search_tweets_correct_ignite(query = 'covid19')

if __name__ == "__main__":
    # calling main function
    main()
