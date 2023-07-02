FROM continuumio/miniconda3:latest

RUN apt-get update \
&& apt-get -y install build-essential \
&& apt-get -y install software-properties-common \
&& wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add - \
&& add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ \
&& apt-get update \
&& apt-get -y install adoptopenjdk-8-hotspot

WORKDIR /code

COPY environment.yml .
RUN conda env create -f environment.yml

COPY ./app /code/app

CMD [ \
  "conda", "run", "--no-capture-output", "-n", "mlflow-env", \
  "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000" \
]
