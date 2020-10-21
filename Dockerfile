FROM php:7.4-zts

RUN pecl install parallel \
    && docker-php-ext-enable parallel
