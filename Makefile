REBAR3 = "rebar3"

all: app

app:
	$(REBAR3) compile

clean:
	$(REBAR3) clean

clean_all:
	$(REBAR3) clean --all

test: app
	$(REBAR3) eunit

shell: app
	$(REBAR3) shell
