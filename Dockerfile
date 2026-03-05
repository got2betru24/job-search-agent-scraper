###################################################
# Stage: scraper-base
###################################################
FROM python:3.13-slim AS scraper-base
WORKDIR /app/scraper

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

###################################################
# Stage: scraper-dev
###################################################
FROM scraper-base AS scraper-dev
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 9000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9000", "--reload"]

###################################################
# Stage: scraper-prod
###################################################
FROM scraper-base AS scraper-prod
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 9000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9000", "--workers", "2"]
