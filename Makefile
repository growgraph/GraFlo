RUNTEST=python -m unittest discover -v

.PHONY: test
test:
	${RUNTEST} test

.PHONY: black
black:
	black -l 79 .

.PHONY: mypy
mypy:
	mypy graph_cast

.PHONY: isort
isort:
	isort . --line-length=79


.PHONY: autoflake
autoflake:
	autoflake --remove-unused-variables --verbose --in-place  ./graph_cast/**/*py

all: autoflake black isort mypy

#.PHONY: pylint
#pylint:
#	pylint lm_service


