
# distributed algorithms, n.dulay, 14 jan 24 
# coursework, raft 
# Makefile, v1

SERVERS   = 5      	# 3 or more
CLIENTS   = 5      	# 1 or more
TIMELIMIT = 40000	# milli-seconds(ms) to quit after
PARAMS    = default	# e.g. default, slower, faster, etc

DEBUG_OPTIONS = "!inf !snd ?rec"
DEBUG_LEVEL   = 0

# DEBUG_OPTIONS = "+areq -areq +arep -arep +vreq +vall -vreq +vrep -vrep +atim -atim +etim -etim +dreq -dreq +drep -drep -creq -crep"

# AppendEntries(areq, arep, atim), Vote(vreq, vrep, vall), Election(etim), DB(dreq, drep), Client(creq, crep)
# Plus (+) for send/send_after. Minus (-) for receiver

START     = Raft.start
HOST	 := 127.0.0.1

# --------------------------------------------------------------------

TIME    := $(shell date +%H:%M:%S)
SECS    := $(shell date +%S)
COOKIE  := $(shell echo $$PPID)

NODE_SUFFIX := ${SECS}_${LOGNAME}@${HOST}

ERLANG  := "-kernel prevent_overlapping_partitions false"
ELIXIR  := elixir --no-halt --cookie ${COOKIE} --erl ${ERLANG} --name
MIX 	:= -S mix run -e ${START} \
	${NODE_SUFFIX} ${TIMELIMIT} ${DEBUG_LEVEL} ${DEBUG_OPTIONS} \
	${SERVERS} ${CLIENTS} ${PARAMS}

# --------------------------------------------------------------------

run: 	compile
	@ echo -------------------------------------------------------------------
	@for k in `seq 1 ${SERVERS}`; do \
                (${ELIXIR} server$${k}_${NODE_SUFFIX} ${MIX} cluster_wait &) ; \
        done

	@for k in `seq 1 ${CLIENTS}`; do \
                (${ELIXIR} client$${k}_${NODE_SUFFIX} ${MIX} cluster_wait &) ; \
        done

	@sleep 3
	@ ${ELIXIR} raft_${NODE_SUFFIX} ${MIX} cluster_start


compile:
	mix compile

clean:
	mix clean
	@rm -f erl_crash.dump

ps:
	@escho ------------------------------------------------------------
	epmd -names




