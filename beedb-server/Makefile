.PHONY: compile test classpath package run1 run2 run3 up down restart status tail logclean clean

MVN      := mvn -q
LOG_DIR  := logs
CP_FILE  := target/classpath.txt
MAIN     := com.memcache.Server
# the same self-contained jar that gets deployed: running it here means local
# and server start the node exactly the same way, dependencies included.
JAR      := target/beedb-server-1.0-SNAPSHOT.jar

# logback.xml reads -Dnode.id to name its rolling file appender, so each node
# writes its own logs/<nodeId>.log with rotation and a totalSizeCap.
# stdout is sent to /dev/null: logback owns the file now, and a shell redirect
# writing to the same path would fight the rolling policy.
JAVA_RUN = java -Dnode.id=$(1) -jar $(JAR) $(1).properties

# wipe one node's logs so every start writes into an empty file: the live file,
# the rotated .gz archives, and any .tmp left behind by an interrupted roll.
CLEAN_LOGS = rm -f $(LOG_DIR)/$(1).log $(LOG_DIR)/$(1)-metrics.log \
                   $(LOG_DIR)/$(1).*.log.gz $(LOG_DIR)/$(1)-metrics.*.log.gz \
                   $(LOG_DIR)/$(1).*.tmp $(LOG_DIR)/$(1)-metrics.*.tmp

# find a node's live JVMs by command line rather than by pid file. The pid file
# is not authoritative: `up` overwrites it, so a node left running by an earlier
# up/run* becomes untrackable and survives every restart still holding its
# ports. Matching -Dnode.id=<node> finds those orphans too, and cannot be fooled
# by a recycled pid the way a stale pid file can.
NODE_PIDS = pgrep -f "Dnode\.id=$(1) -jar .*beedb-server.*\.jar"

# both ports a node binds, read from its .properties so this can never drift
# from the real config.
NODE_PORTS = grep -hE '^(clientPort|raftPort)=' $(1).properties | cut -d= -f2 | tr -d ' \r'

# ports currently in LISTEN state, one per line, IPv4/IPv6 prefix stripped.
LISTENING = ss -ltnH 2>/dev/null | awk '{print $$4}' | sed 's/.*://'

compile:
	$(MVN) clean compile

test:
	$(MVN) test

# resolve the dependency classpath once so `up`/`run*` don't pay Maven's
# per-invocation bootstrap cost for every node.
classpath: compile
	$(MVN) dependency:build-classpath -Dmdep.outputFile=$(CP_FILE)

# build the shaded jar. tests are skipped here on purpose: this is the inner
# loop, `mvn verify` is where tests belong.
package:
	$(MVN) -DskipTests package

run1: package
	@mkdir -p $(LOG_DIR)
	@$(call CLEAN_LOGS,node1)
	$(call JAVA_RUN,node1)

run2: package
	@mkdir -p $(LOG_DIR)
	@$(call CLEAN_LOGS,node2)
	$(call JAVA_RUN,node2)

run3: package
	@mkdir -p $(LOG_DIR)
	@$(call CLEAN_LOGS,node3)
	$(call JAVA_RUN,node3)

# start all 3 nodes as background processes. logs are written by logback
# (logs/node<N>.log); only pids land here directly.
up: package
	@mkdir -p $(LOG_DIR)
	@busy=""; \
	listening=$$($(LISTENING)); \
	for n in 1 2 3; do \
		for p in $$($(call NODE_PORTS,node$$n)); do \
			if echo "$$listening" | grep -qx "$$p"; then \
				owner=$$(ss -lptnH "sport = :$$p" 2>/dev/null | grep -o 'pid=[0-9]*' | head -1); \
				echo "node$$n: port $$p already bound ($$owner)"; \
				busy="$$busy node$$n"; \
			fi; \
		done; \
	done; \
	if [ -n "$$busy" ]; then \
		echo "refusing to start -- run 'make down' first"; \
		exit 1; \
	fi
	@for n in 1 2 3; do \
		$(call CLEAN_LOGS,node$$n); \
		nohup java -Dnode.id=node$$n -jar $(JAR) node$$n.properties \
			> /dev/null 2>&1 & echo $$! > $(LOG_DIR)/node$$n.pid; \
	done
	@sleep 1
	@echo "Cluster up. node1=11211 node2=11212 node3=11213 (client ports)"
	@$(MAKE) --no-print-directory status

down:
	@for n in 1 2 3; do \
		rm -f $(LOG_DIR)/node$$n.pid; \
		pids=$$($(call NODE_PIDS,node$$n) || true); \
		if [ -z "$$pids" ]; then echo "node$$n: nothing running"; continue; fi; \
		for pid in $$pids; do kill $$pid 2>/dev/null || true; done; \
		i=0; \
		while [ $$i -lt 20 ]; do \
			alive=$$($(call NODE_PIDS,node$$n) || true); \
			[ -z "$$alive" ] && break; \
			sleep 0.5; i=$$((i+1)); \
		done; \
		for pid in $$($(call NODE_PIDS,node$$n) || true); do \
			echo "node$$n: pid $$pid ignored SIGTERM, sending SIGKILL"; \
			kill -9 $$pid 2>/dev/null || true; \
		done; \
		echo "stopped node$$n (pid $$(echo $$pids))"; \
	done

restart: down up

status:
	@for n in 1 2 3; do \
		pids=$$($(call NODE_PIDS,node$$n) || true); \
		if [ -z "$$pids" ]; then echo "node$$n: DOWN"; else \
			echo "node$$n: RUNNING (pid $$(echo $$pids))"; \
		fi; \
	done

# Wipe all three nodes' logs without starting anything.
logclean:
	@mkdir -p $(LOG_DIR)
	@for n in 1 2 3; do $(call CLEAN_LOGS,node$$n); done
	@echo "logs cleared for node1 node2 node3"

tail:
	tail -f $(LOG_DIR)/node1.log $(LOG_DIR)/node2.log $(LOG_DIR)/node3.log

# dropping rotated archives too, not just the live logs.
clean: down
	$(MVN) clean
	rm -rf $(LOG_DIR)

# run the framing probe against a live cluster.
#
# the probe is a test class, so it still runs from target/test-classes with the
# dependency classpath -- only the nodes moved to the jar.
#
#   make up && make probe
probe:
	$(MVN) test-compile
	$(MVN) dependency:build-classpath -Dmdep.outputFile=$(CP_FILE)
	java -cp "target/classes:target/test-classes:$$(cat $(CP_FILE))" com.memcache.FramingProbe
