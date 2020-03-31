FROM python:3.7.7-slim-buster

WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY . .

ENV MONGO_URL mongodb://clustering_mongodb:27017/

EXPOSE 8000
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]