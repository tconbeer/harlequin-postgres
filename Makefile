.PHONY: check
check:
	ruff format .
	ruff . --fix
	mypy
	pytest

.PHONY: init
init:
	docker-compose up -d

.PHONY: clean
clean:
	docker-compose down

.PHONY: serve
serve:
	harlequin -P None -a postgres "postgresql://postgres:for-testing@localhost:5432/postgres"

.PHONY: psql
psql:
	PGPASSWORD=for-testing psql -h localhost -p 5432 -U postgres

profile.html: $(wildcard src/**/*.py)
	pyinstrument -r html -o profile.html --from-path harlequin -a postgres "postgresql://postgres:for-testing@localhost:5432/postgres"
