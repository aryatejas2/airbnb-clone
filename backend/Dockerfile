FROM python:3.6
EXPOSE 5000

ENV PYTHONDONTWRITEBYTECODE 1
#ENV PYTHONUNBUFFERED 1

ADD . /app
WORKDIR /app
#RUN python -m pip install -r requirements.txt
COPY * /app/
RUN pip install -r requirement.txt

CMD python app.py

