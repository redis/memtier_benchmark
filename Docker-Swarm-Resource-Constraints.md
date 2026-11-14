# Docker Swarm Resource Constraints Manual

This manual explains how to configure **CPU**, **Memory**, and **CPU pinning** constraints when running services in **Docker Swarm**.

---

## 1. Introduction

In Docker Swarm, you do **not** use the classic `docker run` flags (`--memory`, `--cpus`, `--cpuset-cpus`).

Instead, you configure resource constraints on **Services** using:

- `docker service create`
- `docker service update`
- or in a Compose file with `docker stack deploy`

### Key Concepts

| Term              | Meaning                                      | Behavior when exceeded          |
|-------------------|----------------------------------------------|---------------------------------|
| **Limit**         | Hard maximum                                 | Memory → OOM Kill<br>CPU → Throttled |
| **Reservation**   | Guaranteed minimum (used by scheduler)       | Soft guarantee                  |

---

## 2. Basic Commands

### Create a service with resource limits

```bash
docker service create \
  --name my-app \
  --replicas 2 \
  --limit-cpu 1.0 \
  --limit-memory 512M \
  --reserve-cpu 0.5 \
  --reserve-memory 256M \
  nginx:alpine
```

### Update an existing service

```bash
docker service update \
  --limit-cpu 1.5 \
  --limit-memory 1G \
  --reserve-cpu 0.5 \
  --reserve-memory 512M \
  my-app
```

### Remove resource constraints

```bash
docker service update \
  --limit-cpu -1 \
  --limit-memory -1 \
  --reserve-cpu -1 \
  --reserve-memory -1 \
  my-app
```

---

## 3. Available Flags

| Flag                  | Description                          | Example          |
|-----------------------|--------------------------------------|------------------|
| `--limit-cpu`         | Hard CPU limit (number of cores)     | `1.5`            |
| `--limit-memory`      | Hard memory limit                    | `512M` / `1G`    |
| `--reserve-cpu`       | Guaranteed CPU                       | `0.5`            |
| `--reserve-memory`    | Guaranteed memory                    | `256M`           |
| `--limit-memory-swap` | Total memory + swap limit (optional) | `1G`             |

> **Note**: There is **no direct `--cpuset-cpus`** equivalent in Swarm services.  
> See section 6 for workarounds.

---

## 4. Full Practical Example

### Example 1: Simple Web Service

```bash
docker service create \
  --name web \
  --replicas 3 \
  --limit-cpu 0.5 \
  --limit-memory 256M \
  --reserve-cpu 0.25 \
  --reserve-memory 128M \
  --publish 8080:80 \
  nginx:alpine
```

### Example 2: Redis with memory limit

```bash
docker service create \
  --name redis \
  --replicas 1 \
  --limit-cpu 1.0 \
  --limit-memory 512M \
  --reserve-memory 256M \
  --publish 6379:6379 \
  redis:alpine \
  redis-server --maxmemory 256mb --maxmemory-policy allkeys-lru
```

### Example 3: High-performance application

```bash
docker service create \
  --name api \
  --replicas 4 \
  --limit-cpu 2.0 \
  --limit-memory 2G \
  --reserve-cpu 1.0 \
  --reserve-memory 1G \
  --publish 3000:3000 \
  myapp:latest
```

---

## 5. Using with Docker Compose (Stack Deploy)

You can define the same constraints in a Compose file and deploy it as a stack.

### Example `docker-compose.yml`

```yaml
version: "3.8"

services:
  web:
    image: nginx:alpine
    ports:
      - "8080:80"
    deploy:
      replicas: 3
      resources:
        limits:
          cpus: "0.50"
          memory: 256M
        reservations:
          cpus: "0.25"
          memory: 128M

  redis:
    image: redis:alpine
    command: redis-server --maxmemory 256mb
    ports:
      - "6379:6379"
    deploy:
      replicas: 1
      resources:
        limits:
          cpus: "1.0"
          memory: 512M
        reservations:
          cpus: "0.5"
          memory: 256M
```

### Deploy the stack

```bash
docker stack deploy -c docker-compose.yml mystack
```

### Update the stack

Just edit the file and run the same command again:

```bash
docker stack deploy -c docker-compose.yml mystack
```

---

## 6. CPU Pinning (`cpuset`) in Swarm

Docker Swarm **does not support** `--cpuset-cpus` directly on services.

### Workaround Options

#### Option A: Use Placement Constraints + Node Labels (Recommended)

1. Label your nodes with specific CPU information:

```bash
docker node update --label-add cpu.set=0-3 worker1
docker node update --label-add cpu.set=4-7 worker2
```

2. Force the service to run only on nodes with the desired label:

```bash
docker service create \
  --name pinned-app \
  --constraint 'node.labels.cpu.set == 0-3' \
  --limit-cpu 2 \
  --limit-memory 1G \
  myapp:latest
```

#### Option B: Run on a single-node Swarm + classic `cpuset`

If you only have one node, you can still use the classic Compose syntax with `cpuset`:

```yaml
services:
  app:
    image: myapp
    cpuset: "0,1"
    deploy:
      resources:
        limits:
          memory: 512M
```

---

## 7. How to Verify Resource Constraints

### Inspect a service

```bash
docker service inspect my-app --pretty
```

### Check running tasks

```bash
docker service ps my-app
```

### Live resource usage

```bash
docker stats
```

### Detailed container info

```bash
docker inspect <container_id> --format='Memory: {{.HostConfig.Memory}} | NanoCPUs: {{.HostConfig.NanoCpus}}'
```

---

## 8. Best Practices

1. **Always set memory limits** — this protects the host from OOM situations.
2. **Set reservations lower than limits** — allows bursting while guaranteeing minimum resources.
3. **Sum of all reservations** on a node should stay below the node’s available resources.
4. **Use application-level limits** together with container limits (e.g. Redis `--maxmemory`, Java `-Xmx`, Node.js `--max-old-space-size`).
5. **Monitor with `docker stats`** and adjust values based on real usage.
6. Prefer **Compose + `docker stack deploy`** for complex applications instead of long `docker service create` commands.

---

## 9. Quick Reference

```bash
# Create with limits
docker service create --name app \
  --limit-cpu 1 --limit-memory 512M \
  --reserve-cpu 0.5 --reserve-memory 256M \
  nginx

# Update limits
docker service update --limit-cpu 2 --limit-memory 1G app

# Deploy from Compose
docker stack deploy -c docker-compose.yml mystack

# Check
docker service inspect app --pretty
docker stats
```

