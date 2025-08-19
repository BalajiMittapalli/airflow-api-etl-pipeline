up:
	docker-compose up --build

down:
	docker-compose down

airflow-init:
	docker-compose run --rm airflow-init
