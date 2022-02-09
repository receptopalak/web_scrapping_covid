import psycopg2
import requests
from bs4 import BeautifulSoup

page = requests.get('https://covid19asi.saglik.gov.tr/')

soup = BeautifulSoup(page.text, 'html.parser')

artist_name_list = soup.find(class_='svg-turkiye-haritasi-tamamlanan')
artist_name_list_items = artist_name_list.find_all("text")
artist_list_items = artist_name_list.g("data-adi")
artist_list_items_toplam = artist_name_list.g("data-toplam")
artist_list_items_birinci = artist_name_list.g("data-birinci-doz")
artist_list_items_ikinci = artist_name_list.g("data-ikinci-doz")



plaka_kodu =[]
for i in range(3, 164,2):
    plaka_kodu.append(i)   

kod =[]
for i in range(0, 80):
    kod.append(i)   

sehir_list = artist_name_list.g.contents
sehir_asi_toplam_list = artist_list_items_toplam 
sehir_asi_birinci_list = artist_list_items_birinci
sehir_asi_ikinci_list = artist_list_items_ikinci 
sehirlist = sehir_list[3]["data-adi"]

asi_sehir = []
asi_sayi = []
asi_birinci = []
asi_ikinci = []

for sehirler in plaka_kodu:   
    detay= sehir_list[sehirler]["data-adi"]
    detay_toplam_ham = sehir_list[sehirler]["data-toplam"]
    detay_birinci_ham = sehir_list[sehirler]["data-birinci-doz"]
    detay_ikinci_ham = sehir_list[sehirler]["data-ikinci-doz"]

    detay_toplam= detay_toplam_ham.replace('.','')
    detay_birinci= detay_birinci_ham.replace('.','')
    detay_ikinci= detay_ikinci_ham.replace('.','')

    print(detay)    
    print(detay_toplam)
    print(detay_birinci)
    print(detay_ikinci)

    detay_3= sehir_list[sehirler].find_all("text")[0].contents[0]
    detay_2= detay_3.replace('.','')
    #print(detay_2)
    asi_sehir.append(detay)
    asi_sayi.append(detay_toplam)
    asi_birinci.append(detay_birinci)
    asi_ikinci.append(detay_ikinci)
    

print("INSERT INTO public.asi (" + str(asi_sehir[0]) , str(asi_sayi[0]) +" ) VALUES('test', 5);")



print(len(asi_sayi))


for asi_list in asi_sehir:
    sehir_listesi = asi_list
    print(sehir_listesi)




#Establishing the connection
conn = psycopg2.connect(
   database="dummy", user='postgres', password='ntc123*', host='127.0.0.1', port= '5432'
)
#Setting auto commit false
conn.autocommit = True

#Creating a cursor object using the cursor() method
cursor = conn.cursor()




# Preparing SQL queries to INSERT a record into the database.

cursor.execute('''delete from asi where insert_date = now()::date''')
kod =[]
for i in range(0, 81):
    kod.append(i)
    sehir= str(asi_sehir[i])
    sayi= str(asi_sayi[i])
    sayi_birinci= str(asi_birinci[i])
    sayi_ikinci= str(asi_ikinci[i])
    print(sehir,sayi,sayi_birinci,sayi_ikinci)
    cursor.execute('''INSERT INTO public.asi (il ,asi_sayisi,birinci_doz,ikinci_doz,insert_date,insert_time) VALUES('''+''' ' ''' + sehir + '''' ''' +''', ''' + sayi + ''','''+ sayi_birinci + ''',''' + sayi_ikinci + ''', now()::date, now()::time);''')

print('asi aktarım tamamlandı. İnsert başlıyor.')
cursor.execute('''truncate table illere_gore_asi_sayisi_table restart identity; 
INSERT INTO public.illere_gore_asi_sayisi_table
(il, asi_sayisi, poly, nufus, il_adi, birinci_doz, ikinci_doz, birinci_doz_yuzde, ikinci_doz_yuzde)
SELECT il, asi_sayisi, st_simplify(poly,0.03,true), nufus, il_adi, birinci_doz, ikinci_doz, birinci_doz_yuzde, ikinci_doz_yuzde
FROM public.illere_gore_asi_sayisi;''')

print('insert tamamlandı.')

# Commit your changes in the database
conn.commit()
print("Records inserted........")

# Closing the connection
conn.close()




