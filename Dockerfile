FROM redis:8-alpine

EXPOSE 6379

CMD ["redis-server", "--save", "", "--appendonly", "no"]
