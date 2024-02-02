# FROM  --platform=linux/amd64 condaforge/miniforge3:latest
FROM  condaforge/miniforge3:latest
LABEL maintainer "Asher Pembroke <apembroke@gmail.com>"

RUN DEBIAN_FRONTEND="noninteractive" apt-get update && apt-get install -y build-essential

WORKDIR /

# kamodo-core
RUN git clone https://github.com/asherp/kamodo-core.git
RUN pip install -e kamodo-core

WORKDIR /code

COPY requirements.txt .

# use pip's --no-cache-dir option to avoid caching, which can reduce the image size
RUN pip install --no-cache-dir -r requirements.txt

COPY . /code
RUN pip install -e .

CMD mkdocs serve -a 0.0.0.0:8000
