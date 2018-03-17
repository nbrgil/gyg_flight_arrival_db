dev-up:
	docker-compose -f db/docker-compose.yml -p dwdockerenv build postgres
	docker-compose -f db/docker-compose.yml -p dwdockerenv up -d