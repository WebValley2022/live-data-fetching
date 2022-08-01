# Live data fetching

## Flask
Production:
```sh
PG_CONNSTRING=postgresql://postgres:postgres@localhost:5432/webvalley2022 FLASK_ENV="development" FLASKSK_DEBUG=True FLASK_APP=app flask run
```

Debug mode:
```sh
PG_CONNSTRING=postgresql://postgres:postgres@localhost:5432/webvalley2022 python app.py
```

## Database
Postgres:
```sh
psql -U postgres -h localhost webvalley2022
```
