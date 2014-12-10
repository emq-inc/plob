PROJECT = plob
include erlang.mk

test: app
	erl -pa ebin -noshell -s plob test -s init stop
