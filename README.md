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