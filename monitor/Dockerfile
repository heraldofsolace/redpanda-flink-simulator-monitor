FROM python:3.10
ADD monitor.py .
RUN pip install kafka-python
CMD ["python", "-u", "./monitor.py"] 