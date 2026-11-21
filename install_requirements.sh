AIRFLOW_VERSION=3.3.2
PYTHON_VERSION="$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

apt update

apt install python3-pip python3-venv

apt install openjdk-17-jre-headless

pip install --no-cache-dir -r requirements.txt --constraint "${CONSTRAINT_URL}"