CONFIG_PATH=${PWD}/.proglog/

export CONFIG_DIR=${CONFIG_PATH}

.PHONY: init # コマンド実行のためのダミーターゲット名定義
init:
	mkdir -p ${CONFIG_PATH}
	rm ${CONFIG_PATH}/*
	cp test/model.conf ${CONFIG_PATH}
	cp test/policy.csv ${CONFIG_PATH}

.PHONY: gencert
gencert:
	make init
	cfssl gencert \
		-initca test/ca-csr.json | \
		cfssljson \
			-bare ca
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=server \
		test/server-csr.json | \
		cfssljson \
			-bare server
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="root" \
		test/client-csr.json | \
		cfssljson \
			-bare root-client
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="nobody" \
		test/client-csr.json | \
		cfssljson \
			-bare nobody-client
	mv *.pem *.csr ${CONFIG_PATH}

.PHONY: test
test:
	make gencert
	go test -race ./...

.PHONY: compile
compile:
	make gencert
	protoc api/v1/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.
	go mod tidy
