FROM python:3.7
ENV PYTHONUNBUFFERED 1
RUN mkdir /jangowave1
RUN mkdir /jangowave1/static
RUN mkdir /jangowave1/queryapp
RUN mkdir /jangowave1/query
RUN mkdir /jangowave1/templates
WORKDIR /jangowave1
COPY requirements.txt /jangowave1/
RUN pip3 install -r requirements.txt
COPY jangowave/queryapp/ /jangowave1/queryapp/
COPY jangowave/static/ /jangowave1/static/
COPY jangowave/templates/ /jangowave1/templates/
COPY jangowave/query/ /jangowave1/query/
COPY jangowave/ /jangowave1/
