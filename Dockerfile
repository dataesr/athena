FROM python:3.12-slim

# C.UTF-8 existe dans l'image (en_US.UTF-8 n'y est pas installé)
# AWS_CONFIG_FILE : profil « openalex » disponible quel que soit l'utilisateur
ENV LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    PYTHONUNBUFFERED=1 \
    AWS_CONFIG_FILE=/etc/aws/config

# curl : utilisé par le profil AWS « openalex » (docker/aws_config)
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY requirements.txt /src/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# Accès aux snapshots quotidiens OpenAlex par AWS CLI (profil « openalex »,
# commande curl de la documentation OpenAlex) ; la clé reste dans la variable
# d'environnement OPENALEX_API_KEY, elle n'est pas écrite dans l'image
COPY docker/aws_config /etc/aws/config

COPY . /src
