!/bin/bash

# Collect static files
echo "Collect static files"
python3.7 manage.py collectstatic --noinput

# Apply database migrations
echo "Apply database migrations"
python3.7 manage.py migrate

# Start server
echo "Starting server"
python3.7 manage.py runserver 0.0.0.0:8000
