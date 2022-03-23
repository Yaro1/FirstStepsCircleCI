FROM cimg/python:3.9
RUN groupadd -r myuser && useradd -r -g myuser myuser
RUN pip install psycopg2-binary
USER myuser
