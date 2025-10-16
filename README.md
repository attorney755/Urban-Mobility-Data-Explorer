---

# NYC Taxi Trips Dashboard (Urban Mobility Data Explorer)
A web dashboard for exploring taxi trip data with interactive filtering, charts, and maps.

---

## Team Members
- Samuel NIYONKURU  `backend & documentation`
- David NGARAMBE  `backend`
- Attorney Valois NIYIGABA  `frontend`
- Prudence Browns  `frontend & documentation`

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
│   ├── style.css             # Frontend CSS
│   ├── app.js                # Frontend JavaScript
│   └── ...
├── data/
│   ├── raw/                  # Place `train.csv` here
│   ├── processed/            # Place `cleaned_trips.csv` here
│   ├── schema.sql            # Database schema
│   └── data_dump.sql         # Database data dump
├── scripts/
│   ├── data_cleaning.py      # Data cleaning script
│   ├── db_setup.py           # Database setup script
│   └── update_data_cleaning.py # Updated data cleaning script
└── README.md                 # This file

```

---

## Setup Instructions

### 1) Download the Dataset
The dataset files are large and are hosted on Google Drive. Download and place them in the following locations:
- **Train dataset**: [`train.csv`](https://drive.google.com/file/d/1hIwlem1l4fNdSJCi1MiM9QJv3SSWZGJe/view?usp=drive_link) → `./data/raw/train.csv`
- **Cleaned dataset**: [`cleaned_trips.csv`](https://drive.google.com/file/d/17t7OYgXkZPPhE6-x1uHWkh8bqRphL6O1/view?usp=sharing) → `./data/processed/cleaned_trips.csv`

---

### 2) Install Python and Packages
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
- **Ubuntu / Debian**:
  ```bash
  sudo apt update
  sudo apt install python3-pip
  ```
- **macOS**:
  ```bash
  python3 -m ensurepip --upgrade
  # or if using Homebrew-installed Python (recommended):
  brew install python
  ```
- **Windows**:
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
   **Notes**:
   - If `pip` points to Python 2 on your system, use `pip3`.
   - On macOS, you may need to install Xcode command-line tools: `xcode-select --install`.

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

### 3) Install and Configure MySQL
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
CREATE USER 'team2'@'localhost' IDENTIFIED BY 'Alu@2025!';
GRANT ALL PRIVILEGES ON ncy_taxi.* TO 'team2'@'%';
FLUSH PRIVILEGES;
```

#### macOS (Homebrew)
```bash
brew install mysql
brew services start mysql
mysql_secure_installation
```
Then create the database and user as shown in the Ubuntu section.

#### Windows
1. Download and install MySQL from: [MySQL Installer](https://dev.mysql.com/downloads/installer/)
2. Run MySQL Installer and configure a root password.
3. Open MySQL Shell or Workbench and run the same `CREATE DATABASE` and `CREATE USER` commands as above.

---

### 4) Import the Database
After creating the database and user, import the schema and data:

1. **Import the schema**:
   ```bash
   mysql -u team2 -p nyc_taxi < data/schema.sql
   ```

2. **Import the data**:
   ```bash
   mysql -u team2 -p nyc_taxi < data/data_dump.sql
   ```

---

### 5) Configure Environment Variables
Create a `.env` file inside `backend/` with your database connection details:
```
DB_HOST=localhost
DB_USER=team2
DB_PASSWORD=Alu@2025!
DB_NAME=nyc_taxi
DB_PORT=3306
```

---

### 6) Run the Backend
Start the Flask backend locally:
```bash
cd backend
# activate venv if not already active
python app.py
```
The API should be available at `http://localhost:5000` by default. For production, you can run with Gunicorn:
```bash
gunicorn --bind 0.0.0.0:8000 app:app
```

---

### 7) Run the Frontend

The frontend lives in frontend/. For local development, serve files with Python's simple server:

```
cd frontend
python -m http.server 8000
```
Open http://localhost:8000 in your browser.
## Troubleshooting
- **MySQL connection errors**: Ensure MySQL is running and the credentials in `.env` match the user and password you created.
- **`mysql-connector-python` install issues**: This driver is pure Python and should install via pip. If you see SSL or connection issues, check your MySQL server configuration.
- **Pandas memory issues**: Large CSVs can require a lot of RAM. Consider sampling or using chunked reading with `pandas.read_csv(..., chunksize=...)`.

---

## License
This project is licensed under the MIT License.
