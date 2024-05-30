FROM python:3.11

WORKDIR /app

COPY . .
RUN pip install -r requirements.txt

EXPOSE 8501
RUN echo pwd
CMD ["streamlit", "run", "app.py"]
#CMD streamlit run --server.port $PORT app.py