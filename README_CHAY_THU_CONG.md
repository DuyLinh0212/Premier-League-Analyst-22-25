# Premier League Analytics — cài đặt và chạy thủ công

Đối chiếu mã nguồn nhánh `main`, commit `3789b72`, ngày 14/10/2026; pipeline nền của `8d8dced`. Hướng dẫn dùng Windows + Ubuntu WSL2, HDFS 3 DataNode, Kafka, Spark, Airflow, Neon và web Next.js.

**Không dùng `run.py`, `boot.py`, `airflow_local.py`, các file `.cmd`, `start-all.*` hoặc script khởi động tổng hợp.** Các terminal bên dưới gọi trực tiếp CLI chính thức của Hadoop, Kafka, Spark, Airflow, Uvicorn và Next.js. Các module Python phân tích là mã nghiệp vụ của dự án, vẫn phải được Airflow thực thi.


## Giảng viên bắt đầu từ GitHub

1. Clone nhánh `main` theo mục 2.2, không cần folder hoặc môi trường của người làm bài.
2. Chuẩn bị một database Neon PostgreSQL của người chạy và lấy connection string có `sslmode=require`. Không có mật khẩu/token của sinh viên trong repository.
3. Làm các mục 2–3 để cài công cụ và cấu hình; các mục 4–6 chỉ khởi tạo hạ tầng một lần.
4. Mỗi lần chạy: HDFS (mục 5) → Kafka (mục 6) → mô phỏng/Consumer (mục 7) → Airflow/DAG và đồng bộ Neon (mục 8) → API/web (mục 10).
5. Đối chiếu kết quả theo mục 9 và 11. Nếu database mới, code tự tạo schema từ `database/schemas/analytics.sql`; tài khoản database phải có quyền tạo bảng/index.

Cần Internet để clone, cài dependencies và truy cập Neon. Dữ liệu CSV nguồn được lấy từ repository; `appearances.csv` phải giải nén theo mục 2.4 trước khi chạy. Các đường dẫn `$HOME/...` là của tài khoản Ubuntu đang đăng nhập, không phải đường dẫn máy sinh viên. Dành tối thiểu khoảng 8 GB RAM cho WSL và đủ dung lượng trống cho dữ liệu tải về, Bronze/Silver và môi trường; nhu cầu thực tế phụ thuộc các lần chạy.

## 1. Kiến trúc và thứ tự chạy

```text
CSV FBref/Transfermarkt -> HDFS Bronze -> Spark -> HDFS Silver
                                                 |
API mô phỏng -> Kafka -> Spark -> HDFS Kafka ------+-> KNN 2025–2026
                                                 |
                           Airflow -> mô hình/báo cáo -> HDFS Gold -> Neon
                                                                       |
                                                               FastAPI -> Next.js
```

Thứ tự trên máy mới:

1. Cài công cụ và môi trường Python.
2. Bật NameNode và 3 DataNode, chờ HDFS sẵn sàng.
3. Bật Kafka, tạo topic một partition.
4. Bật API mô phỏng, gửi mùa 2025–2026, chạy Spark Consumer ghi HDFS.
5. Bật Airflow, tạo pool, kích hoạt `football_pipeline`.
6. Bật đồng bộ lịch sử Airflow lên Neon.
7. Bật backend và frontend; kiểm tra kết quả và lưu bằng chứng.

**Bật Airflow khác với chạy DAG.** Chỉ bật dịch vụ sẽ chưa tự chạy pipeline batch vì DAG dùng `schedule=None`.

## 2. Yêu cầu và quy ước terminal

| Thành phần | Phiên bản dùng trong hướng dẫn | Vai trò |
|---|---|---|
| Ubuntu WSL2 | Ubuntu trên Windows | Chạy dịch vụ Linux |
| Java | OpenJDK 17 | Hadoop, Kafka, Spark |
| Hadoop | 3.5.0 | NameNode + 3 DataNode |
| Spark | 3.5.9, bản `bin-hadoop3` Scala 2.12 | Chuẩn hóa, ML, streaming |
| Kafka | 4.3.1, KRaft | Truyền các bảng mô phỏng |
| Python runtime | 3.11 | API, phân tích, PySpark |
| Airflow | 3.3.2, Python 3.11 trong venv riêng | Điều phối DAG |
| Node.js | 22.x hoặc phiên bản đáp ứng Next.js ≥20.9 | Frontend |
| PostgreSQL | Neon, chuỗi kết nối riêng | Báo cáo và metadata |

Mặc định các khối `bash` chạy trong Ubuntu WSL. Các khối `powershell` chạy trong Windows PowerShell. Mở terminal riêng cho mỗi dịch vụ chạy foreground; giữ chúng mở.

Hướng dẫn dành cho giảng viên clone repository trên máy riêng; các thư mục nằm trong home của tài khoản Ubuntu. Nếu đã có cụm Hadoop/Kafka, dùng cấu hình hiện có và bỏ qua phần cài mới/format. Không format cụm đang có dữ liệu.

### 2.1. Cài WSL và công cụ nền trên máy mới

PowerShell với quyền Administrator:

```powershell
wsl --install -d Ubuntu
```

Khởi động lại nếu Windows yêu cầu, mở Ubuntu và tạo tài khoản Linux. Trong Ubuntu:

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk curl git unzip jq openssl python3-venv nodejs npm
java -version
node --version
npm --version
```

Nếu Node từ hệ điều hành thấp hơn 20.9, cài Node 22 từ [trang Node.js chính thức](https://nodejs.org/en/download) cho Linux/WSL trước khi tiếp tục; kiểm tra lại `node --version` và `npm --version`. Không lấy Node cũ chỉ vì `npm` chạy được.

Cài uv và Python 3.11 theo hướng dẫn chính thức của Astral:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
uv python install 3.11
```

### 2.2. Lấy mã nguồn

Máy mới, clone một lần:

```bash
mkdir -p $HOME/premier-league-lab
cd $HOME/premier-league-lab
git clone https://github.com/DuyLinh0212/Premier-League-Analyst-22-25.git Premier-League-Analyst-22-25-live
cd Premier-League-Analyst-22-25-live
git switch main
git log -1 --oneline
```

Máy đã có repository: `git status --short`, xử lý/lưu thay đổi của mình trước, rồi `git pull --ff-only origin main`. Không dùng `git reset --hard` để bỏ thay đổi cho nhanh.

### 2.3. Hadoop, Spark, Kafka: tải trên máy mới

Chỉ làm khi chưa có các thư mục này:

```bash
mkdir -p "$HOME/premier-league-lab/tools"
cd "$HOME/premier-league-lab/tools"
curl -fLO https://archive.apache.org/dist/hadoop/common/hadoop-3.5.0/hadoop-3.5.0.tar.gz
curl -fLO https://archive.apache.org/dist/spark/spark-3.5.9/spark-3.5.9-bin-hadoop3.tgz
curl -fLO https://archive.apache.org/dist/kafka/4.3.1/kafka_2.13-4.3.1.tgz
tar -xzf hadoop-3.5.0.tar.gz
tar -xzf spark-3.5.9-bin-hadoop3.tgz
tar -xzf kafka_2.13-4.3.1.tgz
```

Các URL cố định để tái lập môi trường đã sử dụng; đối chiếu checksum tải xuống với file `.sha512` trong cùng thư mục Apache. Không dùng bản Spark Scala 2.13 với các connector `_2.12` của repository.

### 2.4. Hai môi trường Python riêng

Trong Ubuntu, trên máy mới:

```bash
export PROJECT_ROOT=$HOME/premier-league-lab/Premier-League-Analyst-22-25-live
export BASE=/opt/premier-league-airflow
sudo mkdir -p "$BASE"
sudo chown "$(id -u):$(id -g)" "$BASE"
uv venv --python 3.11 "$BASE/venv-runtime"
uv venv --python 3.11 "$BASE/venv-airflow"
uv pip install --python "$BASE/venv-runtime/bin/python" \
  -r "$PROJECT_ROOT/infrastructure/docker/requirements-runtime.txt" \
  -r "$PROJECT_ROOT/infrastructure/docker/requirements-api.txt"
uv pip install --python "$BASE/venv-airflow/bin/python" \
  'apache-airflow==3.3.2' \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.11.txt
```

Không cài requirements API vào venv Airflow: các phiên bản FastAPI/Starlette của hai phần có thể khác nhau. Spark dùng Python runtime riêng qua `SPARK_PYTHON`.

DAG hiện có gọi đường dẫn tuyệt đối `/opt/project`. Trên máy mới tạo symlink, trên máy đã có chỉ kiểm tra:

```bash
if [ ! -e /opt/project ] && [ ! -L /opt/project ]; then
  sudo ln -s "$PROJECT_ROOT" /opt/project
fi
readlink -f /opt/project
```

Kết quả phải là đúng repository đang chạy. Nếu đang trỏ sang dự án khác, xử lý đường dẫn trước khi chạy DAG; không tạo đè symlink một cách mù quáng.

Khôi phục file nguồn lớn được đóng gói gzip, chỉ khi CSV chưa có:

```bash
cd "$PROJECT_ROOT"
if [ ! -f data/raw/transfermarkt/source_tables/appearances.csv ]; then
  mkdir -p data/raw/transfermarkt/source_tables
  gzip -dc data/raw/transfermarkt/appearances.csv.gz \
    > data/raw/transfermarkt/source_tables/appearances.csv
fi
sha256sum data/raw/transfermarkt/source_tables/appearances.csv
find data/raw/fbref -name '*.csv' | wc -l
find data/raw/transfermarkt/source_tables -maxdepth 1 -name '*.csv' | wc -l
```

Checksum `appearances.csv` của bản đối chiếu: `c4ba5327f9005e4a9914c0f78c3b07132ef2cf1a4babda379bbf54ffd207ebec`. Luôn đối chiếu `data/large_sources.json` nếu repository được cập nhật. Đầu vào preflight hiện yêu cầu 579 CSV FBref + 6 CSV Transfermarkt = 585 CSV.

## 3. Cấu hình dùng chung cho từng terminal

Tạo file **chỉ chứa biến cấu hình**, không phải script chạy dịch vụ:

```bash
mkdir -p "$HOME/.config"
nano "$HOME/.config/premier-league.env"
```

Nội dung mẫu; thay DSN bằng chuỗi kết nối Neon của người chạy, không dùng mật khẩu trong tài liệu/chụp màn hình:

```bash
export PROJECT_ROOT=$HOME/premier-league-lab/Premier-League-Analyst-22-25-live
export BASE=/opt/premier-league-airflow
export LAB="$HOME/premier-league-lab/tools"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME="$LAB/hadoop-3.5.0"
export SPARK_HOME="$LAB/spark-3.5.9-bin-hadoop3"
export KAFKA_HOME="$LAB/kafka_2.13-4.3.1"
export SPARK_PYTHON="$BASE/venv-runtime/bin/python"
export PYSPARK_PYTHON="$SPARK_PYTHON"
export PYSPARK_DRIVER_PYTHON="$SPARK_PYTHON"
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_MASTER='local[2]'
export HDFS_URI=hdfs://localhost:9000
export HDFS_BASE_PATH=/premier-league
export NAMENODE_HTTP_URI=http://localhost:9870
export HDFS_MIN_DATANODES=3
export HADOOP_USER_NAME="$(id -un)"
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export APP_NAMESPACE=premier-league-22-25
export NEON_DATABASE_URL='postgresql://USER:PASSWORD@HOST/DB?sslmode=require'
export STATE_DIR="$BASE/state"
export MATCH_MODEL_DIR="$BASE/state/models/match_outcome"
export KNN_OUTPUT_DIR="$BASE/state/models/knn"
export HDFS_BIN="$PROJECT_ROOT/infrastructure/docker/hdfs"
export PYTHONPATH="$PROJECT_ROOT:$PROJECT_ROOT/apps/api:$PROJECT_ROOT/apps/api/football:$PROJECT_ROOT/ml/inference:$PROJECT_ROOT/processing/spark:$SPARK_HOME/python/lib/pyspark.zip:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip"
export PATH="$BASE/venv-airflow/bin:$SPARK_HOME/bin:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
export SPARK_EXTRA_JARS="$PROJECT_ROOT/infrastructure/docker/jars/*.jar"
export AIRFLOW_HOME="$BASE/home"
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_ROOT/orchestration/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__PARALLELISM=2
export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=1
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin
export AIRFLOW__API__HOST=127.0.0.1
export AIRFLOW__API__PORT=8080
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://127.0.0.1:8080/execution/
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$BASE/home/airflow.db"
export AIRFLOW_METADATA_SQLITE="$BASE/home/airflow.db"
export NO_COLOR=1
```

```bash
chmod 600 "$HOME/.config/premier-league.env"
source "$HOME/.config/premier-league.env"
mkdir -p "$AIRFLOW_HOME" "$STATE_DIR" "$MATCH_MODEL_DIR" "$KNN_OUTPUT_DIR" "$PROJECT_ROOT/evidence"
```

**Mỗi terminal Ubuntu mới đều phải `source` file cấu hình này.** Không nhầm `localhost` trong WSL với địa chỉ bên trong container. Tài liệu này chạy trực tiếp WSL, không chạy Docker Compose.

HDFS CLI của dự án dùng thư viện Hadoop client đi kèm Spark, không phải file khởi động. Kiểm tra LF và quyền thực thi:

```bash
head -n 1 "$HDFS_BIN" | cat -v
chmod +x "$HDFS_BIN"
```

Nếu có `^M` ở cuối shebang, chạy `sed -i 's/\r$//' "$HDFS_BIN"`. `HDFS_BIN` và CLI Hadoop server là hai việc khác nhau: khởi động NameNode/DataNode bằng `$HADOOP_HOME/bin/hdfs`.

Kiểm tra 7 Kafka connector JAR đã có trong `infrastructure/docker/jars`. `SPARK_EXTRA_JARS` phải là **glob `*.jar`**, không phải chuỗi tên file ngăn bằng dấu phẩy: code Consumer đang dùng `glob.glob()`.

## 4. Cấu hình HDFS 3 DataNode — chỉ trên máy mới

Nếu đã cài theo hướng dẫn này: NameNode dùng `$HADOOP_HOME/etc/hadoop`, DataNode dùng `$LAB/conf/dn1`, `dn2`, `dn3`. **Không ghi đè các file này trên máy đã có dữ liệu.** Bỏ mục này và sang mục 5.

Máy trống: tạo các thư mục dữ liệu/cấu hình và khai báo NameNode:

```bash
mkdir -p "$LAB/data/namenode" "$LAB/conf" "$LAB/logs" "$LAB/run"
cat > "$HADOOP_HOME/etc/hadoop/core-site.xml" <<'XML'
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
</configuration>
XML
cat > "$HADOOP_HOME/etc/hadoop/hdfs-site.xml" <<XML
<configuration>
  <property><name>dfs.namenode.name.dir</name><value>file://$LAB/data/namenode</value></property>
  <property><name>dfs.namenode.rpc-address</name><value>localhost:9000</value></property>
  <property><name>dfs.namenode.http-address</name><value>127.0.0.1:9870</value></property>
  <property><name>dfs.replication</name><value>2</value></property>
  <property><name>dfs.blocksize</name><value>67108864</value></property>
</configuration>
XML
```

Ba DataNode là ba tiến trình trên **cùng máy**, không phải ba máy vật lý. Mỗi tiến trình cần thư mục dữ liệu, PID, log và cổng riêng:

```bash
for n in 1 2 3; do
  mkdir -p "$LAB/conf/dn$n" "$LAB/data/dn$n" "$LAB/run/dn$n" "$LAB/logs/dn$n"
  cp -a "$HADOOP_HOME/etc/hadoop/." "$LAB/conf/dn$n/"
  data_port=$((9866 + (n-1)*100))
  http_port=$((9864 + (n-1)*100))
  ipc_port=$((9867 + (n-1)*100))
  cat > "$LAB/conf/dn$n/hdfs-site.xml" <<XML
<configuration>
  <property><name>dfs.datanode.data.dir</name><value>file://$LAB/data/dn$n</value></property>
  <property><name>dfs.datanode.address</name><value>0.0.0.0:$data_port</value></property>
  <property><name>dfs.datanode.http.address</name><value>0.0.0.0:$http_port</value></property>
  <property><name>dfs.datanode.ipc.address</name><value>0.0.0.0:$ipc_port</value></property>
  <property><name>dfs.replication</name><value>2</value></property>
  <property><name>dfs.blocksize</name><value>67108864</value></property>
</configuration>
XML
done
```

**Format một lần duy nhất trên cụm mới**. Đã có `current/VERSION` thì không format:

```bash
if [ ! -f "$LAB/data/namenode/current/VERSION" ]; then
  "$HADOOP_HOME/bin/hdfs" namenode -format
else
  echo 'NameNode đã có metadata; bỏ qua format.'
fi
```

## 5. Bật và kiểm tra HDFS bằng lệnh trực tiếp

```bash
source "$HOME/.config/premier-league.env"
mkdir -p "$LAB/run/nn" "$LAB/logs/nn"
HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop" \
HADOOP_PID_DIR="$LAB/run/nn" HADOOP_LOG_DIR="$LAB/logs/nn" \
  "$HADOOP_HOME/bin/hdfs" --daemon start namenode

for n in 1 2 3; do
  mkdir -p "$LAB/run/dn$n" "$LAB/logs/dn$n"
  HADOOP_CONF_DIR="$LAB/conf/dn$n" HADOOP_PID_DIR="$LAB/run/dn$n" \
  HADOOP_LOG_DIR="$LAB/logs/dn$n" \
    "$HADOOP_HOME/bin/hdfs" --daemon start datanode
done

"$HADOOP_HOME/bin/hdfs" dfsadmin -safemode get
"$HADOOP_HOME/bin/hdfs" dfsadmin -report
jps
```

Chờ `Safe mode is OFF` và `Live datanodes (3)`. Sau khởi động lại máy, NameNode có thể cần vài phút phục hồi metadata. Chờ, kiểm tra log; không format để chữa lỗi kết nối.

Mở `http://localhost:9870/dfshealth.html#tab-datanode` và chụp bằng chứng 3 DataNode. Replication 2 là số bản sao của block, không phải số DataNode đang chạy.

## 6. Kafka — cài cấu hình một lần và bật trực tiếp

Máy đã có `$LAB/kafka-data/server.properties` thì dùng lại, không ghi đè. Máy mới tạo file:

```bash
mkdir -p "$LAB/kafka-data/logs"
cat > "$LAB/kafka-data/server.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=localhost:9093
listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
advertised.listeners=PLAINTEXT://localhost:9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs=$LAB/kafka-data/logs
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
EOF
```

Khởi tạo KRaft **chỉ khi chưa có metadata**:

```bash
if [ ! -f "$LAB/kafka-data/logs/meta.properties" ]; then
  cluster_id="$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)"
  "$KAFKA_HOME/bin/kafka-storage.sh" format --standalone \
    -t "$cluster_id" -c "$LAB/kafka-data/server.properties"
else
  echo 'Kafka đã có metadata; không format lại.'
fi
```

Terminal **K — Broker**, giữ mở:

```bash
source "$HOME/.config/premier-league.env"
export KAFKA_HEAP_OPTS='-Xms256m -Xmx512m'
"$KAFKA_HOME/bin/kafka-server-start.sh" "$LAB/kafka-data/server.properties" \
  2>&1 | tee "$PROJECT_ROOT/evidence/kafka-broker.log"
```

Terminal khác, sau khi broker sẵn sàng:

```bash
source "$HOME/.config/premier-league.env"
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic league_table_simulated \
  --partitions 1 --replication-factor 1
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 \
  --describe --topic league_table_simulated
```

Producer của dự án yêu cầu topic đúng **một partition**. Kafka CLI kết nối được chưa đủ: địa chỉ `advertised.listeners` cũng phải truy cập được từ Spark và producer.

## 7. Tạo dữ liệu Kafka bắt buộc trước KNN

### Terminal S — API sinh bảng mô phỏng

```bash
source "$HOME/.config/premier-league.env"
cd "$PROJECT_ROOT"
"$SPARK_PYTHON" -m uvicorn api:app --host 127.0.0.1 --port 8001 \
  2>&1 | tee evidence/league-api.log
```

`api:app` ở đây là `apps/api/football/api.py` nhờ `PYTHONPATH`. Backend phân tích ở mục 10 là `match_outcome.api:app`, cổng 8000, khác dịch vụ này.

### Terminal P — Producer gửi hai mùa

```bash
source "$HOME/.config/premier-league.env"
cd "$PROJECT_ROOT"
"$SPARK_PYTHON" apps/api/football/producer.py \
  --api-url http://localhost:8001/league-table.csv \
  --bootstrap-server localhost:9092 \
  --topic league_table_simulated \
  --start-season 2025-2026 --interval 5s --count 2 \
  --checkpoint-file "$STATE_DIR/league-producer.json" \
  2>&1 | tee evidence/kafka-producer.log
```

Trên topic/checkpoint mới: phải thấy `Published 2025-2026 ... offset 0` và `Published 2026-2027 ... offset 1`. Nếu đã chạy, producer tiếp tục từ mùa sau cùng dựa vào Kafka và checkpoint local, không tự quay lại 2025–2026. Không xóa checkpoint để cố làm log đẹp.

Kiểm tra bản tin Kafka:

```bash
"$KAFKA_HOME/bin/kafka-console-consumer.sh" \
  --bootstrap-server localhost:9092 --topic league_table_simulated \
  --from-beginning --max-messages 2 --timeout-ms 15000
```

### Terminal C — Spark Consumer ghi HDFS

```bash
source "$HOME/.config/premier-league.env"
cd "$PROJECT_ROOT"
jars=$(find infrastructure/docker/jars -maxdepth 1 -name '*.jar' -printf '%p,' | sed 's/,$//')
"$SPARK_HOME/bin/spark-submit" --master 'local[2]' --driver-memory 512m \
  --jars "$jars" apps/api/football/streaming_spark.py \
  --bootstrap-server localhost:9092 --topic league_table_simulated \
  --hdfs-uri "$HDFS_URI" --hdfs-base-path /premier-league \
  --checkpoint-location "$HDFS_URI/premier-league/kafka/checkpoints/league_table_stream" \
  --available-now 2>&1 | tee evidence/kafka-consumer.log
```

Chỉ tiếp tục khi thấy `Committed 2025-2026 to raw CSV and Parquet`. Chạy lại **đúng lệnh Consumer và đúng checkpoint** để kiểm tra replay: các offset cũ đã commit không bị xử lý lại. Đổi topic thì dùng checkpoint khác, không ghép checkpoint của topic cũ với topic mới.

Đường dẫn chuẩn của **code main mới**:

```text
/premier-league/kafka/raw/fbref/2025-2026/01_league/league_table.csv
/premier-league/kafka/processed/facts/league_table/season=2025-2026/
/premier-league/kafka/checkpoints/league_table_stream/
```

`/premier-league/league_simulation/...` là đường dẫn demo riêng từ lần trước, không phải input mặc định của KNN hiện tại. Không dùng hai đường dẫn thay thế lẫn nhau mà không kiểm tra code.

CSV này là **mô phỏng** từ nền 2024–2025, không phải kết quả thật 2025–2026. Checkpoint streaming nhớ Kafka offset; checkpoint KNN lưu fingerprint và cấu hình phân tích, không trực tiếp sinh mùa mới. Producer còn có checkpoint local lưu mùa đã gửi.

## 8. Bật Airflow và chạy pipeline bằng CLI

### Terminal A — dịch vụ Airflow

```bash
source "$HOME/.config/premier-league.env"
cd "$PROJECT_ROOT"
mkdir -p "$AIRFLOW_HOME"
if [ ! -s "$AIRFLOW_HOME/execution-jwt-secret" ]; then
  umask 077
  openssl rand -hex 32 > "$AIRFLOW_HOME/execution-jwt-secret"
fi
export AIRFLOW__API_AUTH__JWT_SECRET="$(cat "$AIRFLOW_HOME/execution-jwt-secret")"
"$BASE/venv-airflow/bin/airflow" standalone \
  2>&1 | tee evidence/airflow-service.log
```

`airflow standalone` là CLI chính thức của Airflow: khởi tạo/migrate metadata và bật các thành phần phục vụ local. Không dùng file chạy tổng hợp của dự án. Cấu hình môi trường ở mục 3 ghi đè các giá trị mặc định trong `airflow.cfg`.

Mở `http://localhost:8080`. Tài khoản `admin`; mật khẩu được Airflow tạo tại `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`. Đọc riêng trên máy, không đưa mật khẩu vào bằng chứng/báo cáo/Git.

### Terminal T — kiểm tra DAG, pool và trigger

```bash
source "$HOME/.config/premier-league.env"
cd "$PROJECT_ROOT"
airflow version
airflow dags list
airflow dags list-import-errors
airflow pools set spark_serial 1 'Một Spark task tại một thời điểm'
airflow dags unpause football_pipeline
airflow dags pause football_kafka_analytics
run_id="manual_main_$(date +%Y%m%d_%H%M%S)"
airflow dags trigger football_pipeline --run-id "$run_id" \
  2>&1 | tee evidence/airflow-trigger.log
printf '%s\n' "$run_id" > evidence/airflow-run-id.txt
```

Chờ `football_pipeline` thành công **11/11 tác vụ** trong UI. Giữ DAG streaming lịch mỗi phút ở trạng thái pause khi đang demo thủ công; pipeline match-stream có cấu hình topic khác và không cần chạy chồng vào demo BXH.

| Thứ tự | Task trong `football.py` | Ý nghĩa |
|---|---|---|
| 1 | `preflight` | Kiểm tra 585 CSV, NameNode và DataNode |
| 2 | `register_raw_snapshot` | Ghi/dùng lại Bronze bất biến và manifest |
| 3 | `build_identity` | Chuẩn hóa mã đội/cầu thủ |
| 4 | `build_silver` | Tạo bảng Parquet Silver |
| 5 | `validate_silver` | Kiểm tra dữ liệu, khóa và các điều kiện chất lượng |
| 6 | `publish_silver` | Công bố Silver, tạo dấu `_VALIDATED` |
| 7 | `knn` | Đọc Silver + Kafka 2025–2026, tạo dự đoán và checkpoint |
| 8 | `match_outcome` | Huấn luyện/đánh giá mô hình trận đấu |
| 9 | `match_forecast_v2` | Mô hình dự đoán trước/trong trận |
| 10 | `publish` | Chuẩn bị metadata công bố |
| 11 | `merged_publish` | Ghi Gold và công bố trọn bộ báo cáo lên Neon |

`orchestration/dags/football.py` khai báo thứ tự; từng task gọi `infrastructure/docker/runtime.py stage <task>`. Đây là **mã nghiệp vụ xử lý công đoạn**, không phải `run.py` khởi động dịch vụ. Không chạy trực tiếp `python football.py` để thay Airflow.

Máy đã có Bronze đúng checksum có thể dùng lại; Silver/model/báo cáo vẫn chạy cho run mới. Bronze khác dữ liệu hoặc thiếu `_READY` sẽ bị từ chối ghi đè; cần tìm nguyên nhân/bản dữ liệu trước, không xóa toàn bộ HDFS.

### Terminal H — đồng bộ lịch sử lên Neon

```bash
source "$HOME/.config/premier-league.env"
cd "$PROJECT_ROOT"
"$SPARK_PYTHON" -m analytics.airflow_history --interval 15 \
  2>&1 | tee evidence/airflow-neon-sync.log
```

Phần này đọc metadata Airflow **SQLite** của môi trường local. `pl_airflow_runs` và `pl_airflow_task_history` lưu trạng thái DAG/lần thử; `pl_analysis_runs` lưu các lần công bố báo cáo. Không áp dụng nguyên lệnh SQLite nếu chuyển Airflow metadata sang PostgreSQL.

## 9. Kiểm tra kết quả HDFS

Các lệnh ở mục này dùng CLI Hadoop chính thức, không qua script khởi động dự án:

```bash
source "$HOME/.config/premier-league.env"
"$HADOOP_HOME/bin/hdfs" dfs -ls /premier-league
"$HADOOP_HOME/bin/hdfs" dfs -ls /premier-league/bronze
"$HADOOP_HOME/bin/hdfs" dfs -cat /premier-league/bronze/manifest.json | head -n 30
"$HADOOP_HOME/bin/hdfs" dfs -ls /premier-league/silver
"$HADOOP_HOME/bin/hdfs" dfs -ls /premier-league/silver/_runs
"$HADOOP_HOME/bin/hdfs" dfs -ls /premier-league/gold
"$HADOOP_HOME/bin/hdfs" dfs -ls /premier-league/checkpoints/analytics/knn_league_table
"$HADOOP_HOME/bin/hdfs" dfs -cat /premier-league/checkpoints/analytics/knn_league_table/latest.json
```

Cấu trúc cần đối chiếu:

```text
/premier-league/
├── bronze/ (_READY, manifest.json)
│   ├── fbref/season=2022-23, season=2023-24, season=2024-25
│   └── transfermarkt/_source + 3 phân vùng mùa
├── silver/
│   ├── matches, players, teams, transfers
│   ├── player_season_stats, team_season_stats
│   └── _runs/<pipeline_run_id>/_VALIDATED
├── gold/
│   ├── league_standings, player_performance, prediction_features
│   ├── season_comparison, tactical_insights
│   └── team_performance, transfer_impact (có thể còn dữ liệu phiên bản cũ)
├── kafka/
│   ├── raw/fbref/<mùa>/01_league/league_table.csv
│   ├── processed/facts/league_table/season=<mùa>/*.parquet
│   └── checkpoints/league_table_stream/(offsets, commits, sources)
└── checkpoints/analytics/knn_league_table/(latest.json, history.jsonl)
```

Không chỉ chụp thư mục: phải chỉ ra file, schema, số dòng và đối chiếu CSV nguồn. Đọc Parquet bằng Spark ở terminal riêng:

```bash
"$SPARK_PYTHON" - <<'PY'
import os
from pyspark.sql import SparkSession
spark=SparkSession.builder.master('local[1]').appName('VerifyLeagueTable').config('spark.hadoop.fs.defaultFS',os.environ['HDFS_URI']).config('spark.hadoop.dfs.client.use.datanode.hostname','true').getOrCreate()
df=spark.read.parquet(os.environ['HDFS_URI']+'/premier-league/kafka/processed/facts/league_table/season=2025-2026')
df.printSchema()
print('Số đội:',df.count())
df.orderBy('rank').show(20,truncate=False)
spark.stop()
PY
```

Phải thấy 20 đội và các trường rank, matches_played, wins, draws, losses, goals_for, goals_against, goal_diff, points, points_per_mp, season. Xem CSV tương ứng:

```bash
"$HADOOP_HOME/bin/hdfs" dfs -cat /premier-league/kafka/raw/fbref/2025-2026/01_league/league_table.csv
```

## 10. Bật backend và frontend trực tiếp

### Terminal B — FastAPI

```bash
source "$HOME/.config/premier-league.env"
cd "$PROJECT_ROOT"
"$SPARK_PYTHON" -m uvicorn match_outcome.api:app --host 127.0.0.1 --port 8000 \
  2>&1 | tee evidence/backend.log
```

`http://localhost:8000/docs` là Swagger. Không dùng API mô phỏng cổng 8001 làm backend web.

### Terminal W — Next.js

Cài dependencies một lần hoặc khi lockfile đổi:

```bash
source "$HOME/.config/premier-league.env"
cd "$PROJECT_ROOT/apps/web"
npm ci
```

Chạy trực tiếp Next.js CLI:

```bash
export FOOTBALL_API_URL=http://127.0.0.1:8000
node node_modules/next/dist/bin/next dev --hostname 127.0.0.1 --port 3000 \
  2>&1 | tee "$PROJECT_ROOT/evidence/frontend.log"
```

Build và chạy bản production bằng CLI nếu cần:

```bash
node node_modules/next/dist/bin/next build
node node_modules/next/dist/bin/next start --hostname 127.0.0.1 --port 3000
```

Không chạy dev và production cùng cổng 3000. Nếu chọn chạy frontend trên Windows, dùng Node/Python đã cài trên Windows và đổi `PYTHONPATH` sang đường dẫn Windows với dấu `;`; không dùng các path `/mnt/d/...` cho Python Windows.

## 11. Kiểm tra Neon, API và bằng chứng

```bash
curl -f http://localhost:8000/health
curl -f 'http://localhost:8000/reports?limit=1'
curl -f 'http://localhost:8000/airflow/history?limit=1'
curl -f 'http://localhost:8000/standings?season=2024-2025'
curl -f http://localhost:8000/knn/predictions
curl -f 'http://localhost:3000/api/football/standings?season=2024-2025'
```

Neon SQL Editor, truy vấn kiểm chứng, không đưa DSN vào ảnh:

```sql
SELECT run_id,status,report_count,created_at,completed_at,error
FROM pl_analysis_runs ORDER BY created_at DESC LIMIT 5;

SELECT dag_id,airflow_run_id,pipeline_run_id,state,started_at,completed_at
FROM pl_airflow_runs ORDER BY started_at DESC LIMIT 5;

SELECT report_name,count(*) FROM pl_analysis_reports
WHERE run_id = 'THAY_PIPELINE_RUN_ID_THUC_TE'
GROUP BY report_name ORDER BY report_name;
```

`airflow_run_id` dạng `manual_main_...` và `pipeline_run_id` dạng `football_<hash>` là hai mã khác nhau. Lấy cặp mã từ `pl_airflow_runs`; không chọn báo cáo của run cũ để chứng minh run mới.

Lần chạy đối chiếu của bản `8d8dced` tạo 22 báo cáo: outlook 9 bộ lọc, phases, match_results, actual_standings 3 mùa, matches, clubs, model, model_metrics, forecast_v2, forecast_v2_metrics, knn_predictions, players. Mỗi BXH thực tế có 20 đội; KNN phải có `query_season=2025-2026` và 20 đội. Số này có thể thay đổi khi code thay đổi.

Log task Airflow nằm trong:

```text
$AIRFLOW_HOME/logs/dag_id=football_pipeline/run_id=<airflow_run_id>/task_id=<task>/attempt=<n>.log
```

Trong UI: `football_pipeline` → chọn run → chọn task → Logs. Log GET `200 OK` của Uvicorn/Next.js chỉ chứng minh API trả lời, không chứng minh DAG hay mô hình đã hoàn tất.

Checklist bằng chứng phù hợp hồ sơ nộp:

- Commit/branch và lệnh đã thực hiện, phiên bản Java/Python/Spark/Airflow.
- NameNode và 3 DataNode, HDFS không ở safe mode.
- Bronze manifest/checksum, Silver schema/số dòng và `_VALIDATED` của run mới.
- Airflow DAG, thời gian đầu/cuối và log 11 tác vụ của cùng một run.
- Kafka topic/partition/offset, producer gửi mùa, Consumer commit, CSV/Parquet tương ứng, checkpoint và replay.
- Spark KNN features, scaler, mùa tham chiếu, query_season, output CSV và checkpoint fingerprint.
- Model metrics/split thời gian và nhận xét giới hạn; không gọi dữ liệu mô phỏng là kết quả thật.
- Neon status `complete`, report_count, run_id đúng; ảnh/video web đọc báo cáo đó.
- Nhật ký lỗi/cách sửa và lịch sử commit/đóng góp thực tế. Ảnh giao diện riêng không thay thế log + nguồn + kết quả.

## 12. Lỗi thường gặp và dừng dịch vụ

| Triệu chứng | Kiểm tra/cách xử lý |
|---|---|
| `NoBrokersAvailable` | Broker chưa sẵn sàng; kiểm tra cổng 9092, listener, metadata và log Kafka; không format lại |
| HDFS `Connection refused` sau bật máy | Chờ NameNode phục hồi; đọc log; kiểm tra Java và port; không format lại |
| HDFS chỉ có 1/2 DataNode | Mỗi DataNode phải có data dir, PID dir, log dir và cổng riêng |
| `Failed to find data source: kafka` | Kiểm tra 7 JAR, Scala 2.12, `--jars`, biến glob `SPARK_EXTRA_JARS` |
| `Query season 2025-2026 has no rows` | Chạy Kafka Consumer, kiểm tra phân vùng dưới `/premier-league/kafka/processed/facts/league_table` |
| `/opt/project` không thấy source | Kiểm tra symlink đúng PROJECT_ROOT; DAG đang gọi path tuyệt đối |
| Không có task chạy, trạng thái queued | DAG đang pause, scheduler/dag-processor chưa chạy hoặc thiếu pool `spark_serial` |
| Web `Not Found` sau pull | Khởi động lại backend; kiểm tra `/openapi.json`, API mới và proxy allowlist |
| Backend `/standings` 200 nhưng web proxy 404 | Bản `8d8dced` quên `standings` trong allowlist của `apps/web/src/app/api/football/[...path]/route.js`; thêm đúng route đọc này, không bỏ toàn bộ allowlist |
| Neon báo lỗi xác thực | Kiểm tra DSN, SSL và database đúng; không in DSN ra log |
| Có thư mục nhưng không có kết quả hợp lệ | Kiểm tra `_READY`, `_VALIDATED`, `_source_sha256`, số dòng và đọc lại Parquet |

Nhánh `main` từ commit `c5d3038` đã bổ sung `standings` trong allowlist. Nếu dùng bản cũ thì cần cập nhật mã nguồn. Không coi thao tác Ctrl+F5 là sửa được API backend/proxy còn thiếu.

Kiểm thử mã (không khởi động tất cả dịch vụ bằng helper):

```bash
source "$HOME/.config/premier-league.env"
cd "$PROJECT_ROOT"
"$SPARK_PYTHON" -m unittest discover -s tests/orchestration -p test_airflow_history.py
"$SPARK_PYTHON" -m unittest discover -s apps/api/football/tests -v
```

Không gọi các fixture local là bằng chứng end-to-end nếu chưa có Kafka/HDFS/Neon thật của lần chạy.

Dừng Uvicorn, Next.js, Kafka và Airflow foreground bằng Ctrl+C trong đúng terminal. Dừng daemon HDFS khi không còn task ghi:

```bash
for n in 1 2 3; do
  HADOOP_CONF_DIR="$LAB/conf/dn$n" HADOOP_PID_DIR="$LAB/run/dn$n" \
  HADOOP_LOG_DIR="$LAB/logs/dn$n" "$HADOOP_HOME/bin/hdfs" --daemon stop datanode
done
HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop" HADOOP_PID_DIR="$LAB/run/nn" \
HADOOP_LOG_DIR="$LAB/logs/nn" "$HADOOP_HOME/bin/hdfs" --daemon stop namenode
```

Không dùng `wsl --shutdown`, xóa thư mục dữ liệu hoặc format làm thao tác dừng bình thường. Sau pull code mới: đọc thay đổi, cài lại dependency nếu lock/requirements đổi, khởi động lại backend/Airflow nếu cần, rồi trigger run mới.

## 13. Nguồn và phạm vi xác minh

- [Airflow 3.3.2 Quick Start](https://airflow.apache.org/docs/apache-airflow/3.3.2/start.html): WSL trên Windows, cài constraints, CLI standalone và cấu hình.
- [uv installation](https://docs.astral.sh/uv/getting-started/installation/).
- [Hadoop 3.5.0 setup](https://hadoop.apache.org/docs/r3.5.0/hadoop-project-dist/hadoop-common/SingleCluster.html).
- [Apache archive: Hadoop](https://archive.apache.org/dist/hadoop/common/hadoop-3.5.0/), [Spark](https://archive.apache.org/dist/spark/spark-3.5.9/), [Kafka](https://archive.apache.org/dist/kafka/4.3.1/).
- Mã đối chiếu: `orchestration/dags/football.py`, `infrastructure/docker/runtime.py`, `apps/api/football/{api,producer,streaming_spark}.py`, `processing/spark/pl_pipeline/{core,silver}.py`, `ml/inference/knn/knn_league_table.py`, `apps/api/analytics/{publish,neon_store,airflow_history}.py`.

Pipeline của commit này đã chạy đủ 11 bước trên môi trường hiện có. Các lệnh cài mới được đối chiếu tài liệu và mã nguồn, **chưa được thử lại toàn bộ trên một máy sạch**. Không format/cài đè môi trường đang chạy chỉ để xác nhận README. Tài liệu không chứa DSN/mật khẩu/token thật và không chứng nhận phần triển khai mà chưa có log/bằng chứng.
