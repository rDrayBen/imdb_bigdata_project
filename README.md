# imdb_bigdata_project

Посилання на гугл диск з [даними](https://drive.google.com/drive/folders/1VRq_HFyYSpFR8-tcqU-iYdP7azLOZTKv?usp=sharing)

Розподіл таблиць:\
Работягов: principals\
Ратушняк: name.basics\
Слободян: episode\
Уфімцева: akas\
Долинська: rating+crew\
Френіс: title.basics

Питання:\
**Работягов**
1. Визначте 10 найкращих режисерів із найвищим середнім рейтингом фільмів для фільмів, що мають щонайменше 1000 голосів.

    Таблиці включені: title.ratings, title.crew, title.Principals, name.basics. \
    Операції: filter, join, group by, window.
2. Перелічіть найбільш роботящих акторів - тих, хто знімався у понад 50 фільмах - з відповідними підрахунками фільмів.

    Таблиці залучаються: title.Principals, name.basics.\
    Операції: filter, group by.
3. Для фільмів, випущених після 2010 року в жанрі "комедії", зазначте 5 найкращих фільмів на основі середнього рейтингу.

    Таблиці включені: title.BASICS, title.rating, title.principals.\
    Операції: filter, window, join.
4. Для кожного фільму перелічіть імена акторів, замовлених за допомогою користувальницького рейтингу (наприклад, порядок їх появи в акторському складі).

    Таблиці, що стосуються: title.Principals, name.basics.\
    Операції: filter, window, group by.
5. Ідентифікувати телесеріал (Titletype = 'TVSeries'), де кількість епізодів перевищує загальну середню кількість епізодів на серіал та перераховуйте назви серіалів із такою кількістю епізодів.

    Таблиці включені: title.Basics, title.Episode, title.principals.\
    Операції: filter, group by, window.
6. Які пари акторів часто з’являються разом у різних фільмах? 
    
    Таблиці, що стосуються: title.Principals, name.BASICS.\
    Операції: filter, group by, window.


**Френіс**
1. Визначити всі фільми (titleType = 'movie'), випущені після 2005 року.  
   - Таблиці: title.basics  
   - Операції: filter  

2. Порахувати кількість фільмів у кожному жанрі.  
   - Таблиці: title.basics  
   - Операції: group by  

3. Знайти середню тривалість фільмів для визначених жанрів (мінімум n фільмів у жанрі) та об'єднати з середнім рейтингом для жанру.  
   - Таблиці: title.basics, title.ratings  
   - Операції: join, group by, filter  

4. Визначити 5 найдовших фільмів (titleType = 'movie') за хронометражем.  
   - Таблиці: title.basics  
   - Операції: filter, window  

5. Визначити роки для кінематографа, де випущено найбільшу кількість фільмів.  
   - Таблиці: title.basics  
   - Операції: filter, group by, window  

6. Знайти n найкращих режисерів для кожного десятиліття за середнім рейтингом у визначених жанрах.  
   - Таблиці: title.basics, title.crew, title.ratings  
   - Операції: join, group by, filter

**Ратушняк**
1. Які актори найчастіше знімаються у фільмах жанру "Action" з рейтингом понад 8.0 та мінімум 5000 голосів?
    Таблиці: name.basics, title.principals, title.basics, title.ratings
    Операції: filter, join, group by

2. Визначити топ-10 письменників за кількістю написаних серій у телесеріалах з жанром "Drama".
    Таблиці: name.basics, title.crew, title.basics, title.episode
    Операції: filter, join, group by

3. Які актори мають найбільшу кількість фільмів, перекладених більш ніж на 10 мов (із title.akas)?
    Таблиці: name.basics, title.principals, title.akas
    Операції: filter, join, group by

4. Які сценаристи писали сценарії для фільмів, що мають оцінку нижче 5.0, але отримали понад 10,000 голосів?
    Таблиці: name.basics, title.crew, title.ratings
    Операції: filter, join, group by

5. Які професії (primaryProfession) у людей найчастіше зустрічаються у короткометражних фільмах (titleType='short')?
    Таблиці: name.basics, title.principals, title.basics
    Операції: filter, join, group by

6. Які сценаристи мали найдовшу серію послідовних років із щонайменше одним випущеним фільмом?
    Таблиці: name.basics, title.crew, title.basics
    Операції: join, filter, group by, window
