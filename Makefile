.PHONY: check
check:
	poetry run ruff format .
	poetry run ruff check . --fix
	poetry run mypy
	poetry run pytest

.PHONY: init
init:
	docker-compose up -d

.PHONY: clean
clean:
	docker-compose down

.PHONY: serve
serve:
	poetry run harlequin -P None -a postgres "postgresql://postgres:for-testing@localhost:5432/postgres"

.PHONY: psql
psql:
	PGPASSWORD=for-testing psql -h localhost -p 5432 -U postgres -E

profile.html: $(wildcard src/**/*.py)
	poetry run pyinstrument -r html -o profile.html --from-path harlequin -a postgres "postgresql://postgres:for-testing@localhost:5432/postgres"
