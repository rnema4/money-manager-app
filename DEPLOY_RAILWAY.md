# Deploy To Railway + MySQL

This project is ready for Railway with:
- `requirements.txt` including Gunicorn
- `Procfile` web start command
- env-based DB config in `app.py`

## 1) Push code to GitHub
Push this folder to a GitHub repository first.

## 2) Create Railway project
1. Open Railway.
2. Click **New Project**.
3. Choose **Deploy from GitHub repo**.
4. Select this repository.

## 3) Add MySQL service
1. In the same Railway project, click **New**.
2. Choose **Database** -> **MySQL**.
3. Railway provisions DB credentials automatically.

## 4) Configure app service env vars
Set these in the app service:
- `SECRET_KEY` = long random value

DB config works with either:
- URL style: `DATABASE_URL` (or `MYSQL_URL` / `MYSQL_PUBLIC_URL`)
- Component style injected by Railway MySQL:
  - `MYSQLHOST`
  - `MYSQLPORT`
  - `MYSQLUSER`
  - `MYSQLPASSWORD`
  - `MYSQLDATABASE`

If `DATABASE_URL` is not set, the app will use component vars automatically.

## 5) Deploy
Railway will build and start:
`web: gunicorn wsgi:app --bind 0.0.0.0:$PORT --workers 2 --threads 4 --timeout 120`

## 6) Post-deploy checks
Open your Railway app URL and verify:
1. Signup/login
2. Add category
3. Add expense
4. Import CSV

The app auto-creates tables on startup (`db.create_all()`).
