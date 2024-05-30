FROM python:3.11

WORKDIR /app

COPY . .
RUN pip install -r requirements.txt

EXPOSE 8501
RUN cd web
RUN streamlit run app.py

#CMD streamlit run --server.port $PORT app.py