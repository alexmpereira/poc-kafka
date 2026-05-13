FROM php:8.2-cli

# Install system dependencies and rdkafka extension
RUN apt-get update && apt-get install -y \
    librdkafka-dev \
    git \
    unzip \
    && pecl install rdkafka \
    && docker-php-ext-enable rdkafka

WORKDIR /app

COPY . .

CMD ["php", "consumer.php"]
