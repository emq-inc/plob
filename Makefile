PROJECT = plob

DEPS = jsx

include erlang.mk

test: clean app
	erl -pa ebin -noshell -s plob test -s init stop
