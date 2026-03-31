FROM public.ecr.aws/lambda/python:3.12

RUN pip install --upgrade pip

COPY requirements_lambda.txt .
RUN pip install -r requirements_lambda.txt --target /var/task --no-cache-dir

RUN python3 -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2'); print('Embedding model cached.')"

RUN python3 -c "from transformers import pipeline; pipeline('zero-shot-classification', model='facebook/bart-large-mnli'); print('Classifier cached.')"

COPY api/ /var/task/api/
COPY engine/ /var/task/engine/
COPY chunker/ /var/task/chunker/
COPY citation/ /var/task/citation/
COPY classifier/ /var/task/classifier/
COPY procurement/ /var/task/procurement/
COPY pipeline/ /var/task/pipeline/
COPY config/ /var/task/config/

CMD ["api.lambda_handler.handler"]
