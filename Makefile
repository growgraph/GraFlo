RUNTEST=python -m unittest discover -v

.PHONY: test
test:
	${RUNTEST} test

.PHONY: black
black:
	black -l 79 --preview .

.PHONY: mypy
mypy:
	mypy graph_cast

.PHONY: isort
isort:
	isort . --line-length=79


.PHONY: autoflake
autoflake:
	autoflake --remove-unused-variables --verbose --in-place  ./graph_cast/**/*py

.PHONY: prettyyaml
prettyyaml:
	find . -name "*yaml" -and -not -ipath './.*' -type f -and -not -name "*sizes*" | xargs pretty-format-yaml --autofix --indent 4

.PHONY: prettyjson
prettyjson:
	find . -name "*json" -and -not -ipath './.*' -type f | xargs pretty-format-json --autofix --indent 4

all: autoflake black isort mypy prettyyaml prettyjson

#.PHONY: pylint
#pylint:
#	pylint package_name


