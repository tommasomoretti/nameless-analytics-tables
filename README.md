<picture>
  <source srcset="https://github.com/user-attachments/assets/6af1ff70-3abe-4890-a952-900a18589590" media="(prefers-color-scheme: dark)">
  <img src="https://github.com/user-attachments/assets/9d9a4e42-cd46-452e-9ea8-2c03e0289006">
</picture>

---

# Tables
The Nameless Analytics tables is a set of tables in BigQuery where users, sessions and events data are stored.

For an overview of how Nameless Analytics works [start from here](https://github.com/tommasomoretti/nameless-analytics/).

Table of contents:
- Tables
  - [Main table](#main-table)
  - [User and sessions](#user-and-sessions)
  - [Batch data loader logs](#batch-data-loader-logs)
  - [Dates table](#dates-table)
- Table functions
  - [Users raw latest](#users-raw-latest)
  - [Users](#users)
  - [Sessions](#sessions)
  - [Pages](#pages)
  - [Transactions](#transactions)
  - [Products](#products)
  - [Shopping stages open funnel](#shopping-stages-open-funnel)
  - [Shopping stages closed funnel](#shopping-stages-closed-funnel)
  - [GTM performances](#gtm-performances)
  - [Consents](#consents)
- [Create tables](#create-tables)
- [Create table functions](#create-table-functions)



## Tables
### Main table
This is the schema of the raw data main table. It's a partitioned table by event_date, clustered by client_id, session_id and event_name.


### Main table
Lorem ipsum


### User and sessions
Lorem ipsum 


### Batch data loader logs
Lorem ipsum


### Dates table
Lorem ipsum 



## Table functions
### Users raw latest
Lorem ipsum

| Colonna                       | Descrizione                              |
|-------------------------------|----------------------------------------- |
| user_date                     | Data associata all’utente                |
| user_id                       | Identificativo utente                    |
| client_id                     | Identificativo client                    |
| user_first_session_timestamp  | Timestamp della prima sessione utente    |
| user_last_session_timestamp   | Timestamp dell’ultima sessione utente    |
| days_from_first_to_last_visit | Giorni tra prima e ultima visita         |
| days_from_first_visit         | Giorni dalla prima visita a oggi         |
| days_from_last_visit          | Giorni dall’ultima visita a oggi         |
| user_channel_grouping         | Raggruppamento canale utente             |
| user_source                   | Fonte utente                             |
| user_campaign                 | Campagna utente                          |
| user_campaign_id              | ID campagna utente                       |
| user_device_type              | Tipo dispositivo utente                  |
| user_country                  | Paese utente                             |
| user_language                 | Lingua utente                            |
| session_date                  | Data della sessione                      |
| session_id                    | ID sessione                              |
| session_number                | Numero sessione                          |
| cross_domain_session          | Flag sessione cross-domain               |
| session_start_timestamp       | Timestamp inizio sessione                |
| session_end_timestamp         | Timestamp fine sessione                  |
| session_duration_sec          | Durata sessione in secondi               |
| session_channel_grouping      | Raggruppamento canale sessione           |
| session_source                | Fonte sessione                           |
| session_campaign              | Campagna sessione                        |
| session_campaign_id           | ID campagna sessione                     |
| session_device_type           | Tipo dispositivo sessione                |
| session_country               | Paese sessione                           |
| session_language              | Lingua sessione                          |
| session_hostname              | Hostname sessione                        |
| session_browser_name          | Nome browser sessione                    |
| session_landing_page_category | Categoria pagina di atterraggio sessione |
| session_landing_page_location | URL pagina di atterraggio sessione       |
| session_landing_page_title    | Titolo pagina di atterraggio sessione    |
| session_exit_page_category    | Categoria pagina di uscita sessione      |
| session_exit_page_location    | URL pagina di uscita sessione            |
| session_exit_page_title       | Titolo pagina di uscita sessione         |


### Users
Lorem ipsum


### Sessions
Lorem ipsum


### Pages
Lorem ipsum


### Transactions
Lorem ipsum


### Products
Lorem ipsum


### Shopping stages open funnel
Lorem ipsum


### Shopping stages closed funnel
Lorem ipsum


### GTM performances
Lorem ipsum


### Consents
Lorem ipsum



## Create tables



## Create table functions


---

Reach me at: [Email](mailto:hello@tommasomoretti.com) | [Website](https://tommasomoretti.com/?utm_source=github.com&utm_medium=referral&utm_campaign=nameless_analytics) | [Twitter](https://twitter.com/tommoretti88) | [Linkedin](https://www.linkedin.com/in/tommasomoretti/)
