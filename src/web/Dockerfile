FROM python:3.11 AS web

COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt

CMD ["python", "web.py"]