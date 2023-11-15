FROM python:3.11 AS filter

COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt

CMD ["python", "filter.py"]
