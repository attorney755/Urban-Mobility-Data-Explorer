# NYC Taxi Trips Dashboard (Urban Mobility Data Explorer)

A web dashboard for exploring taxi trip data with interactive filtering, charts, and maps.

## Team Members
- Samuel NIYONKURU
- David NGARAMBE
- Attorney Valois NIYIGABA
- Prudence Browns

---

## Project Structure

```
nyc-taxi-dashboard/
├── backend/
│   ├── app.py                # Flask backend
│   ├── requirements.txt      # Backend dependencies
│   └── ...
├── frontend/
│   ├── index.html            # Frontend HTML
│   ├── app.js                # Frontend JavaScript
│   └── ...
├── data/
│   ├── raw/                  # Place `train.csv` here
│   ├── processed/            # Place `cleaned_trips.csv` here
│   └── links/                # Google Drive links for datasets
├── scripts/
│   └── data_cleaning.py      # Data cleaning script
└── README.md                 # This file
```

---

## Setup Instructions

Follow these steps to get the project running locally. The backend requires Python 3.8+.

### 1) Download the dataset

The dataset files are large and are hosted on Google Drive. Download and place them in the following locations:

- Train dataset: `./data/raw/train.csv`  (https://drive.google.com/file/d/1hIwlem1l4fNdSJCi1MiM9QJv3SSWZGJe/view?usp=drive_link)
- Cleaned dataset: `./data/processed/cleaned_trips.csv`  (https://drive.google.com/file/d/17t7OYgXkZPPhE6-x1uHWkh8bqRphL6O1/view?usp=sharing)

---

### 2) Install Python and packages

The backend and scripts use the dependencies listed in `backend/requirements.txt`:

```
flask==2.3.2
flask-cors==3.0.10
mysql-connector-python==8.0.33
gunicorn==20.1.0
pandas==2.0.3
```

You can install packages globally, inside a virtual environment, or using conda. Below are OS-specific instructions for creating an environment and installing dependencies.

#### Install pip (if missing)

Most modern Python installations include `pip`. If `pip` is not available on your system, install it as follows:

- Ubuntu / Debian:

```bash
sudo apt update
sudo apt install python3-pip
```

- macOS:

```bash
python3 -m ensurepip --upgrade
# or if using Homebrew-installed Python (recommended):
brew install python
```

- Windows:

```powershell
python -m ensurepip --upgrade
# or download and run get-pip.py from https://bootstrap.pypa.io/get-pip.py
```

After installing `pip`, you can continue with the virtual environment steps below.

#### macOS / Linux (recommended: venv)

1. Create and activate a virtual environment (Python 3.8+):

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install backend packages:

```bash
cd backend
pip install -r requirements.txt
```

Notes:
- If `pip` points to Python 2 on your system, use `pip3`.
- On macOS you may need to install Xcode command-line tools for compiling some packages: `xcode-select --install`.

#### Windows (recommended: venv)

1. Create and activate a virtual environment (PowerShell example):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2. Install backend packages:

```powershell
cd backend
pip install -r requirements.txt
```

If you use Command Prompt instead of PowerShell, activate the venv with `.\.venv\Scripts\activate`.

---

### 3) Install and configure MySQL (mysql-connector-python explained)

The project uses MySQL. Below are installation steps for common OSes, plus notes about the Python driver `mysql-connector-python` (already included in `requirements.txt`).

#### Why `mysql-connector-python`?

`mysql-connector-python` is Oracle's official pure-Python MySQL driver. It's easy to install via pip and does not require compiling native C extensions.

#### Ubuntu / Debian

```bash
sudo apt update
sudo apt install mysql-server
sudo systemctl start mysql
sudo mysql_secure_installation
```

Then create the database and a user (interactive or run these SQL commands):

```sql
CREATE DATABASE nyc_taxi;
CREATE USER 'nyc_user'@'localhost' IDENTIFIED BY 'your_password';
GRANT ALL PRIVILEGES ON nyc_taxi.* TO 'nyc_user'@'localhost';
FLUSH PRIVILEGES;
```

Connect from Python using the credentials above. Example (in `backend/app.py` or scripts) will use environment variables.

#### macOS (Homebrew)

```bash
brew install mysql
brew services start mysql
mysql_secure_installation
```

Then create the database and user as shown in the Ubuntu section.

#### Windows

1. Download and install MySQL from: https://dev.mysql.com/downloads/installer/
2. Run MySQL Installer and configure a root password.
3. Open MySQL Shell or Workbench and run the same `CREATE DATABASE` and `CREATE USER` commands as above.

Note: After installing MySQL, `mysql-connector-python` can be installed in your Python environment with `pip install mysql-connector-python` (already covered when installing `requirements.txt`). No extra OS-level dependencies are required for this driver.

---

### 4) Configure environment variables

Create a `.env` file inside `backend/` with your database connection details (or export these variables in your shell):

```
DB_HOST=localhost
DB_USER=nyc_user
DB_PASSWORD=your_password
DB_NAME=nyc_taxi
DB_PORT=3306
```

If your `app.py` reads environment variables differently, update these names accordingly.

### DB testing & development credentials

For development and quick testing the backend currently uses a hard-coded connection in `backend/app.py`:

```python
host='localhost',
user='team2',
password='Alu@2025!',
database='nyc_taxi'
```

These credentials were included to make initial local testing easier. For any deployment or shared repository you MUST remove hard-coded credentials and use environment variables (for example, the `.env` file described above) or a secrets manager. If you want, I can change `backend/app.py` to read those values from environment variables and add a short `backend/.env.example` file.

---

### 5) Run the backend

Start the Flask backend locally:

```bash
cd backend
# activate venv if not already active
python app.py
```

The API should be available at `http://localhost:5000` by default.

For production you can run with Gunicorn (installed via `requirements.txt`):

```bash
gunicorn --bind 0.0.0.0:8000 app:app
```

---

### 6) Run the frontend

The frontend lives in `frontend/`. For local development, serve files with Python's simple server:

```bash
cd frontend
python -m http.server 8000
```

Open `http://localhost:8000` in your browser.

If your frontend uses a build tool (e.g., npm / webpack), follow that tool's steps instead.

---

## Troubleshooting

- MySQL connection errors: ensure MySQL is running and the credentials in `.env` match the user and password you created. Use `sudo systemctl status mysql` (Linux) or check Homebrew services (macOS).
- `mysql-connector-python` install issues: this driver is pure Python and should install via pip. If you see SSL or connection issues, check your MySQL server configuration to ensure it allows local TCP connections and the user has proper grants.
- Pandas memory issues: large CSVs can require a lot of RAM. Consider sampling or using chunked reading with `pandas.read_csv(..., chunksize=...)`.

---

## License

This project is licensed under the MIT License.

---

## Notes & Next Steps

- Replace placeholder dataset and video links with actual URLs.
- If you want, I can also add a short `backend/README.md` with example `.env` values and a sample curl request to test the API.


