FROM python:slim-buster

WORKDIR /app
COPY . .

RUN apt-get update && \ 
    python -m pip install -r requirements.txt && \
    apt-get clean

#ENV AWS_DEFAULT_REGION=
#ENV AWS_ACCESS_KEY_ID=
#ENV AWS_SECRET_ACCESS_KEY=
EXPOSE 8000

CMD ["gunicorn", "-w", "4", "-k", "uvicorn.workers.UvicornH11Worker", "app.zarrdap:app", "-b", "0.0.0.0:8000", "--access-logfile", "/app/access.log", "--error-logfile", "/app/error.log", "--forwarded-allow-ips", "*"]