NO_SERVER ?= 1
SERVER_PORTS ?= 8080

ETCD_IMAGE = gcr.io/etcd-development/etcd:v3.5.7
ETCD_CONTAINER = my-etcd

MEMORY_LIMIT = 512m
CPU_LIMIT = 1

PORT_2379 = 2379
PORT_2380 = 2380

CLIENT = client/cmd/main.go
SERVER = server/cmd/main.go

BUILD_DIR := build
LOGS_DIR := logs
PID_DIR := .pids

$(shell mkdir -p $(PID_DIR) $(LOGS_DIR))

build-server:
	@echo "Building server ..."
	go build -o $(BUILD_DIR)/server $(SERVER)

build-client:
	@echo "Building client ..."
	go build -o $(BUILD_DIR)/client $(CLIENT)

start-servers: build-server
	@echo "Starting $(NO_SERVER) servers..."
	@i=1; \
	for port in $(SERVER_PORTS); do \
		if [ $$i -le $(NO_SERVER) ]; then \
			log_file=$(LOGS_DIR)/server_$$port.log; \
			echo "Starting server $$i on port $$port. Logs will be stored in $$log_file"; \
			./$(BUILD_DIR)/server -addr=0.0.0.0:$$port -cAddr="http://0.0.0.0:$(PORT_2379),http://0.0.0.0:$(PORT_2380)" > $$log_file 2>&1 & \
			pid=$$!; \
			if [ $$? -eq 0 ]; then \
				echo $$pid > $(PID_DIR)/server_$$port.pid; \
				echo "Server $$i started with PID $$pid on port $$port. Logs stored in $$log_file"; \
			else \
				echo "Error starting server $$i"; \
			fi; \
		fi; \
		i=$$((i + 1)); \
	done

stop-servers:
	@echo "Stopping all servers..."
	@for pid_file in $(PID_DIR)/*.pid; do \
		if [ -f $$pid_file ]; then \
			pid=$$(cat $$pid_file); \
			if [ -n "$$pid" ] && ps -p $$pid > /dev/null 2>&1; then \
				echo "Stopping server with PID $$pid"; \
				kill $$pid; \
				rm -f $$pid_file; \
			else \
				echo "No process found for PID $$pid, skipping kill"; \
			fi; \
		fi; \
	done
	
	rm -rf $(LOGS_DIR)
	rm -rf $(BUILD_DIR)
	rm -rf $(PID_DIR)
	rm -rf "gdq"

	@echo "All servers stopped and directories cleaned."

start-etcd-server:
	@echo "Starting etcd server..."
	docker run -d \
		--name $(ETCD_CONTAINER) \
		-p $(PORT_2379):$(PORT_2379) \
		-p $(PORT_2380):$(PORT_2380) \
		--memory=$(MEMORY_LIMIT) \
		--cpus=$(CPU_LIMIT) \
		$(ETCD_IMAGE) \
		/usr/local/bin/etcd \
		--name $(ETCD_CONTAINER) \
		--data-dir /etcd-data \
		--listen-client-urls http://0.0.0.0:$(PORT_2379) \
		--advertise-client-urls http://0.0.0.0:$(PORT_2379)

stop-etcd-server:
	@echo "Stopping etcd server..."
	docker stop $(ETCD_CONTAINER) || true
	docker rm $(ETCD_CONTAINER) || true

start: start-etcd-server build-client start-servers 
stop: stop-servers stop-etcd-server
