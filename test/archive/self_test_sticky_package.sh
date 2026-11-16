

cd /root/leetcode/Cpp_code/socket
pkill -9 -f bench_server 2>/dev/null; sleep 0.3; rm -f /tmp/glue.log
./build/bench_server --server=epoll --port=6200 > /tmp/glue.log 2>&1 &
for i in $(seq 1 50); do grep -q "model" /tmp/glue.log && break; sleep 0.1; done

for r in 1 2 3 4 5; do
  for i in {1..10}; do printf "hello\n"; done | nc -N 127.0.0.1 6200 > /dev/null
done

echo "===== 发了 50 条 hello，服务端 recv 了几次 ====="
grep -c 'recv]' /tmp/glue.log
echo "===== 每次 recv 的 n 值 ====="
grep -o 'recv] n=[0-9]*' /tmp/glue.log
pkill -9 -f bench_server 2>/dev/null; true



