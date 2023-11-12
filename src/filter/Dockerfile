FROM python:3.11

RUN mkdir /src
COPY . /src
WORKDIR /src
RUN pip install -r requirements.txt

ENTRYPOINT ["python", "filter.py"]
