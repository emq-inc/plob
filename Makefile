REBAR3 = "rebar3"

all: app

app:
	$(REBAR3) compile

clean:
	$(REBAR3) clean

clean_all:
	$(REBAR3) clean --all

test: app
	erl -pa `$(REBAR3) path` -noshell -s plob_compile test -s plob_query test -s init stop

shell: app
	$(REBAR3) shell
