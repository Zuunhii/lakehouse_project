# ------- config -------
NET ?= lakehouse_trino-network
TRINO_CONTAINER ?= TRINO-COORDINATOR

.PHONY: up down restart trino minio trino-ui logs metadata-db \
        setup-containers wait-trino schemas schemas-check

# 1) Lên toàn bộ (build nếu cần) — KHÔNG tạo schema
setup-containers:
	docker compose up -d --build

# (shortcut)
up: setup-containers

# 2) Đợi Trino thật sự "tỉnh" + iceberg catalog sẵn sàng (dùng khi chuẩn bị tạo schema)
wait-trino:
	@echo ">> wait: Trino + iceberg catalog"
	@docker exec $(TRINO_CONTAINER) sh -lc ' \
		for i in `seq 1 300`; do \
		  OUT=$$(trino --server http://localhost:8080 -e "SHOW CATALOGS" 2>/dev/null || true); \
		  echo "$$OUT" | grep -q "^iceberg$$" && { echo "   ok: iceberg ready"; exit 0; }; \
		  echo "   ... initializing, retry $$i"; \
		  sleep 2; \
		done; \
		echo "!! timeout: Trino/iceberg not ready"; exit 1 \
	'

# 3) Tạo 3 schema Iceberg (s3a) — có thể chạy bất cứ lúc nào SAU khi Trino sẵn sàng
schemas:
	@echo ">> create schemas (s3a)"
	@docker exec $(TRINO_CONTAINER) trino --server http://localhost:8080 \
		-f /run-trino-sql/create-schemas.sql
	@echo ">> schemas done"

# 4) Kiểm tra lại
schemas-check:
	@docker exec -it $(TRINO_CONTAINER) trino --server http://localhost:8080 \
		-e "SHOW SCHEMAS FROM iceberg;"

# ---------- tiện ích ----------
down:
	docker compose down

restart:
	docker compose down && docker compose up -d

trino:
	docker container exec -it $(TRINO_CONTAINER) trino

minio:
	xdg-open "http://localhost:9001" 2>/dev/null || open "http://localhost:9001"

trino-ui:
	xdg-open "http://localhost:8080" 2>/dev/null || open "http://localhost:8080"

logs:
	docker logs $(TRINO_CONTAINER)

metadata-db:
	docker exec -ti mariaDB /usr/bin/mariadb -padmin
