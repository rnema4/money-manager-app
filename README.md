# Personal Money Manager (Flask)

A Flask-based personal money manager with authentication, category/account management, statement import, and analytics dashboards.

## Features
- Login/signup with session auth
- Add/edit/delete transactions
- Category management (including custom icon + color)
- CSV/XLSX bank statement import
- Dashboard + analytics views
- MySQL support (Railway-ready) with SQLite fallback for local quick start

## Tech Stack
- Flask + Flask-SQLAlchemy
- SQLAlchemy + PyMySQL
- Pandas + openpyxl (statement parsing)
- Bootstrap + custom CSS

## Local Setup
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### Database config options
The app resolves DB settings in this order:
1. `DATABASE_URL`
2. `MYSQL_URL` / `MYSQL_PUBLIC_URL` / `RAILWAY_DATABASE_URL`
3. Component vars: `MYSQLHOST`, `MYSQLPORT`, `MYSQLUSER`, `MYSQLPASSWORD`, `MYSQLDATABASE`
4. Fallback: local SQLite database at `instance/money_manager.db`

Example (MySQL URL style):
```bash
set DATABASE_URL=mysql+pymysql://root:password@localhost:3306/money_manager
```

Example (component style):
```bash
set MYSQLHOST=localhost
set MYSQLPORT=3306
set MYSQLUSER=root
set MYSQLPASSWORD=password
set MYSQLDATABASE=money_manager
```

## Run
```bash
python app.py
```

Open: `http://127.0.0.1:5000`

## Railway Deploy
- `requirements.txt` already includes production dependencies.
- `Procfile` is included: `gunicorn wsgi:app ...`.
- Add env vars in Railway:
  - `SECRET_KEY` (required)
  - `DATABASE_URL` (or Railway MySQL plugin vars)
- Railway will inject `PORT`; app supports it automatically.

## Import file expectations
Recommended columns:
- Date
- Description
- Amount
- Type (optional: income/expense/transfer or Dr/Cr style)
- Category (optional)

`.csv` and `.xlsx` are supported from the Import page.
