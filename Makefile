dev-up:
	docker-compose -f db/docker-compose.yml -p dwdockerenv build postgres
	docker-compose -f db/docker-compose.yml -p dwdockerenv up -d

start-postgres:
	docker-compose -f docker-compose.yml -p dwdockerenv build postgres
	docker-compose -f docker-compose.yml -p dwdockerenv -d run postgres

run:
	docker rm -f flight_arrival_etl
	docker-compose -f docker-compose.yml -p dwdockerenv build flight_arrival_etl
	docker-compose -f docker-compose.yml -p dwdockerenv up flight_arrival_etl

