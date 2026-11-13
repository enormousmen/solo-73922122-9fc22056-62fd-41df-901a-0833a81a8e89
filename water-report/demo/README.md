# 可控演示脚本

两个脚本针对本次修复的两类建批事故，前提都是
`docker compose up -d` 已把环境拉起（控制台 A :8000、控制台 B :8002、旧站 :8001）。
脚本 1 依赖 curl 与 docker compose；脚本 2 依赖 curl 与 python3（宿主机）。

## 1. 建批中途退出 —— `mid-create-exit.sh`

```bash
./demo/mid-create-exit.sh
```

演示要点：

1. 控制台 A 以上传表单同款接口、用 `demo_delay=60` 慢速建批；
   建批进行中，批次在 A、B 两个控制台的列表里都**不出现**（未发布不可查询）；
2. `docker compose kill reporter-a` 模拟进程中断 —— 半批在 B 上依然
   不可见、不可报送；
3. `docker compose up -d reporter-a` 重启 —— 启动清理把未发布原件
   （含 `incoming/*.part` 残片）与草稿行回收；
4. 重启前已发布的批次完好保留（成功批次重启保留）。

预期结尾输出 `PASS`。

## 2. 同名并发上传 —— `concurrent-same-name.sh`

```bash
./demo/concurrent-same-name.sh
```

演示要点：

1. 构造两份**同名不同内容**的 `same-name.csv`，同一时刻分别上传到
   控制台 A 与控制台 B（两者共享同一 SQLite 与上传目录）；
2. 验证生成两个不同批次，两份原件以不同存储名分别保留、互不覆盖，
   库中摘要与各自原件一致，批次行来自各自原件；
3. 两个批次先后开始报送（共享库全局单任务），旧站最终收到全部 3 条记录。

预期结尾输出 `PASS`。

## 备注

- 两个脚本只读改演示数据，可重复运行；批次、原件会留在共享卷中，
  `docker compose down -v` 可全部清空。
- 环境变量可改地址：`CONSOLE_A_URL`、`CONSOLE_B_URL`、`LEGACY_URL`。
