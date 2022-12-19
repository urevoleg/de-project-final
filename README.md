Переводите ваши денежки:

![](https://srochnyj-kredit.ru/images_article-2/finans-kredit-02.jpg)

# Fintech

**ASIS**
```markdown
Команда аналитиков попросила собрать данные по транзакционной активности пользователей и настроить обновление таблицы с курсом валют.
**Цель** — понять, как выглядит динамика оборота всей компании и что приводит к его изменениям.
```

**Sources**

_1. Backend  мобильного приложения, PostgreSQL. Таблицы:_

Таблицы:
1. `public.transactions` - таблица транзакций,  содержит в себе информацию о движении денежных средств между клиентами в разных валютах:
   2. `operation_id` - id транзакции
   3. `account_number_from` - внутренний бухгалтерский номер счёта транзакции ОТ КОГО
   4. `account_number_to` - внутренний бухгалтерский номер счёта транзакции К КОМУ
   5. `currency_code` - трёхзначный код валюты страны, из которой идёт транзакция
   6. `country` -  страна-источник транзакции
   7. `status` - статус проведения транзакции: `queued` («транзакция в очереди на обработку сервисом»), `in_progress` («транзакция в обработке»), `blocked` («транзакция заблокирована сервисом»), `done` («транзакция выполнена успешно»), `chargeback` («пользователь осуществил возврат по транзакции»)
   8. `transaction_type` - тип транзакции во внутреннем учёте: `authorisation` («авторизационная транзакция, подтверждающая наличие счёта пользователя»), `sbp_incoming` («входящий перевод по системе быстрых платежей»), `sbp_outgoing` («исходящий перевод по системе быстрых платежей»), `transfer_incoming` («входящий перевод по счёту»)
   9. `transfer_outgoing` - `c2b_partner_incoming` («перевод от юридического лица»), `c2b_partner_outgoing` («перевод юридическому лицу»).
   10. `amount` - целочисленная сумма транзакции в минимальной единице валюты страны (копейка, цент, куруш)
   11. `transaction_dt` - дата и время исполнения транзакции до миллисекунд
12. `public.сurrencies` - справочник, который содержит в себе информацию об обновлениях курсов валют и взаимоотношениях валютных пар друг с другом:
    13. `date_update` - дата обновления курса валют
    14. `currency_code` - трёхзначный код валюты транзакции
    15. `currency_code_with` - отношение другой валюты к валюте трёхзначного кода
    16. `currency_code_div` - значение отношения единицы одной валюты к единице валюты транзакции


**Архитектура решения**

![fintech-arch.png](https://storage.yandexcloud.net/public-bucket-6/final-prj/fintech-arch.png)

**ps:** из-за особенностей VERTICA загружать данные в таблицы эффективнее крупными блоками (использование `COPY`), поэтому будет реализована
выгрузка из PostgreSQL в локальные файлы с последующей загрузкой в VERTICA.

В качестве BI используется Metabase.

--------------------------------------------------
![](http://risovach.ru/upload/2017/04/mem/gendalf_143791524_orig_.jpg)
**pps:** из коробки Metabase не умеет ходить в Vertica, для изобретения ходулей использовались:
- [Инструкция с офф сайта metabase](https://www.metabase.com/docs/latest/databases/connections/vertica#adding-the-vertica-jdbc-driver-jar-to-the-metabase-plugins-directory)
- [Драйвер скачен отсюда](https://dbschema.com/jdbc-driver/Vertica.html)

После нехитрых поисков, папка `plugins` в образе расположена по пути `/opt/metabase/plugins`. Скаченный драйвер
скопирован в папку, vertica успешно подключена:
![metabase-vertica-dwh.png](https://storage.yandexcloud.net/public-bucket-6/final-prj/metabase-vertica-dwh.png)

---------------------------------------------------

# План решения

![plan.png](https://storage.yandexcloud.net/public-bucket-6/final-prj/plan.png)

1. Создать необходимые слои в DWH:
   2. `STAGING` - логика загрузки, **asis** - данные из источника в исходном виде загружаются в таблицы staging слоя
   3. `DWH` - datamart layer. Здесь будет расположена требуемая заказчиком витрина `global_metrics`
4. Пайплан загрузки из источников:
   5. DAG Airflow c ежедневным запуском и инкрементальной загрузкой данных за предыдущий день
6. Пайплайн инкрементального обновления витрины

Витрина `global_metrics` должна иметь следующие поля:
- `date_update` - дата расчета
- `currency_from` - код валюты транзакции
- `amount_total` - общая сумма транзакций по валюте в долларах **(?за доллар принимаем код USA?)**
- `cnt_transactions` - общий объём транзакций по валюте;
- `avg_transactions_per_account` - средний объём транзакций с аккаунта
- `cnt_accounts_make_transactions` - количество уникальных аккаунтов с совершёнными транзакциями по валюте

**ASIS**: витрина должна отвечать на вопросы:
- какая ежедневная динамика сумм переводов в разных валютах;
- какое среднее количество транзакций на пользователя;
- какое количество уникальных пользователей совершают транзакции в валютах;
- какой общий оборот компании в единой валюте.

--------------------------------------------------

# Миграции

![migrations.jpg](https://storage.yandexcloud.net/public-bucket-6/final-prj/migrations.jpg)

1. `transactions`
```sql
CREATE TABLE  UREVOLEGYANDEXRU__STAGING.transactions (
	operation_id varchar(60) NULL,
	account_number_from int NULL,
	account_number_to int NULL,
	currency_code int NULL,
	country varchar(30) NULL,
	status varchar(30) NULL,
	transaction_type varchar(30) NULL,
	amount int NULL,
	transaction_dt timestamp NULL
)
order by transaction_dt
SEGMENTED BY hash(operation_id, transaction_dt::date) all nodes
PARTITION BY transaction_dt::date
GROUP BY calendar_hierarchy_day(transaction_dt::date, 3, 2);
```

2. `currencies`
```sql
CREATE TABLE UREVOLEGYANDEXRU__STAGING.currencies (
	date_update timestamp NULL,
	currency_code int NULL,
	currency_code_with int NULL,
	currency_with_div numeric(5, 3) NULL
)
order by date_update
SEGMENTED BY hash(date_update::date) all nodes
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);
```

3. `global_metrics`
```sql
CREATE TABLE IF NOT EXISTS UREVOLEGYANDEXRU__DWH.global_metrics (
	date_update timestamp NOT NULL,
	currency_from varchar NULL,
	cnt_transactions int NULL ,
	amount_total numeric(20, 3) NULL,
	avg_transactions_per_account numeric(5, 3) NULL,
	cnt_accounts_make_transactions int NULL
)
order by date_update
SEGMENTED BY hash(date_update::date) all nodes
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);
```

# Pipeline загрузки из источников

Разработан DAG Airflow для ежедневной загрузки в из источников в хранилище, схема показана на рисунке:
![dag-stg.png](https://storage.yandexcloud.net/public-bucket-6/final-prj/dag-stg.png)

Выполняемые таски:
- формирование выгрузки из источника данных за указанный день
- сохранение в локальный файл
- проверка, что файлы существуют
- загрузка из локального файла в хранилище

DAG успешно отрабатывает:
![dag-stg-workflow.png](https://storage.yandexcloud.net/public-bucket-6/final-prj/dag-stg-workflow.png)

Данные в хранилище доступны:
![metabase-transactions-dwh.png](https://storage.yandexcloud.net/public-bucket-6/final-prj/metabase-transactions-dwh.png)


Схема полного DAG c миграциями и пайплайном загрузки в STG:
![dag-migrations-stg.png](https://storage.yandexcloud.net/public-bucket-6/final-prj/dag-migrations-stg.png)

# Витрина `global_metrics`

**ASIS**

Исходные положения перед расчетом:
- исключаются тестовые аккаунты `account_number_from < 0`
- уникальность `operation_id` не понятна. На скрине ниже, для одного и того же operation_id есть два перевода разным лицам
![not_unique_operation_id.png](https://storage.yandexcloud.net/public-bucket-6/final-prj/not_unique_operation_id.png). Пока будет использоваться как есть.
**UPD: Авторы должны поправить**


Вопросы, возникшие после небольшого исследования данных:
- тк каждая операция содержит лог смены статусов, то возможно необходимо использовать только определенные статусы: `done`, `chargeback`
- + к п.1 использовать только последний статус транзакции


SQL скрипт наполнения витрины:
```sql
MERGE INTO UREVOLEGYANDEXRU__DWH.global_metrics tgt
USING
	(WITH last_status_transactions as(-- last status
										SELECT *,
											   ROW_NUMBER () OVER(PARTITION BY t.operation_id ORDER BY transaction_dt desc) AS rn
										FROM UREVOLEGYANDEXRU__STAGING.transactions t
										WHERE t.account_number_from >= 0)
		SELECT t.transaction_dt::date AS date_update,
			   t.currency_code AS currency_from,
			   sum(t.amount * c.currency_with_div) AS amount_total,
			   count(*) AS cnt_transactions,
			   round(1.0 * count(*) / count(DISTINCT t.operation_id), 2) AS avg_transactions_per_account,
			   count(DISTINCT t.operation_id) AS cnt_accounts_make_transactions
		FROM last_status_transactions t
		JOIN UREVOLEGYANDEXRU__STAGING.currencies c 
		ON t.transaction_dt::date = c.date_update::date
		AND t.currency_code = c.currency_code_with
		WHERE t.rn = 1 AND t.status IN ('done', 'chargeback')
		AND c.currency_code = 420
		GROUP  BY 1, 2
		ORDER BY 1) src
ON tgt.date_update = src.date_update AND tgt.currency_from = src.currency_from
WHEN MATCHED AND (tgt.amount_total <> src.amount_total OR tgt.cnt_transactions <> src.cnt_transactions OR tgt.avg_transactions_per_account <> src.avg_transactions_per_account OR tgt.cnt_accounts_make_transactions <> src.cnt_accounts_make_transactions)
	THEN UPDATE SET amount_total = src.amount_total ,cnt_transactions = src.cnt_transactions ,avg_transactions_per_account = src.avg_transactions_per_account ,cnt_accounts_make_transactions = src.cnt_accounts_make_transactions
WHEN NOT MATCHED
    THEN INSERT (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
    VALUES (src.date_update, src.currency_from, src.amount_total, src.cnt_transactions, src.avg_transactions_per_account, src.cnt_accounts_make_transactions); 
```

## Pipeline

Обновление витрины реализовано при помощи отдельного DAG `cdm-global_metrics`:
![dag-cdm.png](https://storage.yandexcloud.net/public-bucket-6/final-prj/dag-cdm.png)

Данные загружаются, с учетом описанных особенностей данные витрины выглядят так:


## Dashboard

Реализуем дашборд, который ответит на следующие вопросы:
- какая ежедневная динамика сумм переводов в разных валютах
- какое среднее количество транзакций на пользователя
- какое количество уникальных пользователей совершают транзакции в валютах
- какой общий оборот компании в единой валюте

Для удобства пользователя создан словарик валют и их человекочитаемых кодов:
![currencies_dict.png](https://storage.yandexcloud.net/public-bucket-6/final-prj/currencies_dict.png)

Реализованный дашборд:
![dash-global_metrics.png](https://storage.yandexcloud.net/public-bucket-6/final-prj/dash-global_metrics.png)
---------------------------------------------------

Смотрим с перспективой на всё сделанное и наслаждаемся своей улетностью:
![](https://simplywallpaper.net/pictures/2015/11/Kung-Fu-Panda-Po-12.jpg)