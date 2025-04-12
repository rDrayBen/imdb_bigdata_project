# imdb_bigdata_project

Посилання на гугл диск з [даними](https://drive.google.com/drive/folders/1VRq_HFyYSpFR8-tcqU-iYdP7azLOZTKv?usp=sharing)

Розподіл таблиць:\
Работягов: title.principals\
Ратушняк: name.basics\
Слободян: episode\
Уфімцева: akas\
Долинська: rating+crew\
Френіс: title.basics


Питання:\
**Работягов**
1. Визначте 10 найкращих режисерів із найвищим середнім рейтингом(середнім рейтингом серед всіх вибраних фільмів цього режисера) фільмів для фільмів, що мають щонайменше 1000 голосів. +
2. Перелічіть найбільш роботящих акторів - тих, хто знімався у понад 50 фільмах - з відповідними підрахунками фільмів.
3. Для фільмів, випущених після 2010 року в жанрі "комедії", зазначте 5 найкращих фільмів на основі середнього рейтингу. Вибрані фільми мають НЕ мати російських адаптацій. +-
4. Для кожного фільму перелічіть імена акторів, замовлених за допомогою користувальницького рейтингу (наприклад, порядок їх появи в акторському складі).
5. Ідентифікувати телесеріали (Titletype = 'TVSeries'), де кількість епізодів перевищує загальну середню кількість епізодів на серіал із рейтингом більше N. +-
6. Які пари акторів часто з’являються разом у різних фільмах? 


**Френіс**
1. Визначити всі фільми (titleType = 'movie'), випущені після 2005 року.  
2. Порахувати кількість фільмів у кожному жанрі.  
3. Знайти середню тривалість фільмів для визначених жанрів (мінімум n фільмів у жанрі) та об'єднати з середнім рейтингом для жанру.  
4. Визначити 5 найдовших фільмів (titleType = 'movie') за хронометражем.  
5. Визначити роки для кінематографа, де випущено найбільшу кількість фільмів.  
6. Знайти n найкращих режисерів для кожного десятиліття за середнім рейтингом у визначених жанрах для якогось регіону(декілька). + 


**Ратушняк**
1. Які актори найчастіше знімаються у фільмах жанру "Action" з рейтингом понад 8.0 та мінімум 5000 голосів? +
2. Визначити топ-10 письменників за кількістю написаних серій у телесеріалах з жанром "Drama".
3. Які актори мають найбільшу кількість фільмів, перекладених більш ніж на 10 мов (із title.akas)? +-
4. Які сценаристи писали сценарії для фільмів, що мають оцінку нижче 5.0, але отримали понад 10,000 голосів? +
5. Які професії (primaryProfession) у людей найчастіше зустрічаються у короткометражних фільмах (titleType='short')?
6. Які сценаристи мали найдовшу серію послідовних років із щонайменше одним випущеним фільмом? +


**Уфімцева**

Обрані:
* Які актори найчастіше асоціюються з фільмами з високим рейтингами, і як варіюється їхня участь залежно від регіону? (Таблиці 'title.akas', 'title.ratings' та 'title.principals').
* Як змінювався середній рейтинг фільмів в залежності від мови за останні 5 років порівняно з середнім рейтингом цих фільмів за весь час? (Таблиці 'title.basics', 'title.ratings', 'title.akas').

1. У яких регіонах за останні 5 років було випущено найбільше оригінальних творів, і чи є зв’язок між їх кількістю та середніми оцінками? +-
   - Таблиці: 'title.akas', 'title.basics'
   - Операції: filter, join, group by, count, order by
2. Які локалізовані (альтернативні) назви фільмів і телевізійних шоу за останні 5 років отримали найвищі оцінки, і як вони ранжуються в кожному регіоні за середнім рейтингом?
   - Таблиці: 'title.akas', 'title.ratings'
   - Операції: where, join, order by, window function RANK()
3. Які актори найчастіше асоціюються з фільмами з високим рейтингами, і як варіюється їхня участь залежно від регіону? +-
   - Таблиці: 'title.akas', 'title.ratings', 'name.basics' та 'title.principals'
   - Операції: join, where, group by, count, order by
4. Які фільми мають найвищі середні рейтинги в кожному регіоні, якщо розподілити їх за квартилями?
    - Таблиці: 'title.akas', 'title.basics', 'title.ratings'
    - Операції: join, group by, avg, window function NTILE()
5. Як змінювався середній рейтинг фільмів в залежності від мови за останні 5 років порівняно з середнім рейтингом цих фільмів за весь час? +
   - Таблиці: 'title.akas', 'title.ratings' та 'title.basics'
   - Операції: join, filter, group by, aggregation
6. Чи існує взаємозв'язок між оригінальною мовою фільму та кількістю його перекладів у різних регіонах? +-
   - Таблиці: 'title.akas', title.basics
   - Операції: join, filter, group by, count

Номери питань з:
- *filter*: 1, 5, 6
- *join*: 1, 2, 3, 4, 5, 6
- *group by*: 1, 3, 4, 5, 6
- *window function*: 2, 4


 **Долинська**
1. Визначити топ-N країн, фільми яких мають найвищу середню оцінку?
2. Які фільми отримали найбільшу кількість голосів користувачів (за рейтингом або кількістю відгуків) у кожному десятилітті?
3. Які режисери мають найвищу середню оцінку серед фільмів, випущених у різних жанрах?(можливо ще відфільтрувати за мінімальною кількістю знятих фільмів)
4. Як змінювалося середня оцінка для усіх фільмів для режисерів (топ-N за кількістю фільмів або за найвищим середнім рейтингом) протягом кожних 5-ти років їхньої кар'єри (порівняно з їхньою загальною середньою оцінкою за всю кар'єру)? +
5. Які жанри мають найвищий середній рейтинг (середня оцінка) та як змінювався їхній середній рейтинг протягом кожного десятиліття? +-
6. Які актори найчастіше з’являються у найнерейтинговіших фільмах (за середньою оцінкою)? +-


**Слободян**
1. Які серіали мають найбільш стабільний високий рейтинг серед своїх епізодів?
2. Які серіали мають найбільшу кількість локалізованих назв, що може вказувати на міжнародний успіх?
3. Які режисери найчастіше працюють із тим самим акторським складом у різних серіалах? +
4. Які серіали мають найбільшу кількість сезонів із негативною динамікою рейтингу?
5. Які серіали мають найбільшу кількість нагородних епізодів? +-
6. Які жанри серіалів мають найнижчий відсоток завершених проєктів? +-


**Вибрані найкращі питання для демонстрації**:
1. Для фільмів, випущених після 2010 року в жанрі "комедії", зазначте 5 найкращих фільмів на основі середнього рейтингу. Вибрані фільми мають НЕ мати російських адаптацій.(містить операції filter, window, join, group by)
2. Як змінювалося середня оцінка для усіх фільмів для режисерів (топ-N за кількістю фільмів або за найвищим середнім рейтингом) протягом кожних 5-ти років їхньої кар'єри (порівняно з їхньою загальною середньою оцінкою за всю кар'єру)?(містить операції filter, join, window, group by)
3. Які актори найчастіше асоціюються з фільмами з високим рейтингом серед багатьох регіонів, і як їх поява у фільмі змінює рейтинг залежно від регіону?(містить операції filter, join, group by)
4. Як змінився середній рейтинг фільмів для кожної мови (на основі оригінальних назв) за останні 5 років порівняно з середнім рейтингом за весь час для цієї мови?(містить операції filter, join, group by)
5. Які жанри(top N) мають найвищий середній рейтинг (середня оцінка) та як змінювався їхній середній рейтинг протягом кожного десятиліття?(містить операції filter, join, group by)
6. Які актори найчастіше з'являються у найнерейтинговіших фільмах (із середньою оцінкою нижче 5.0), але які при цьому входять до топ-N фільмів за кількістю відгуків?(містить операції join, group by)
