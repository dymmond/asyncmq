# Security & Compliance

Welcome to the chapter where we lock down your AsyncMQ pipeline tighter than Fort Knox—without losing the wit
and professionalism that makes our docs delightful. In this guide, we’ll cover:

1. **Authentication & Authorization**
2. **Encryption (In Transit & At Rest)**
3. **Secure Backend Configuration**
4. **Audit Logging & Tamper Evidence**
5. **Compliance Considerations (GDPR, HIPAA, PCI DSS)**
6. **Best Practices & Common Pitfalls**

---

## 1. Authentication & Authorization

Securing who can enqueue and process jobs is critical—lest random scripts start scheduling chaos.

### 1.1. Who Can Enqueue Jobs?

* **Internal Apps**: Only microservices with valid credentials (JWT, mTLS, or API keys) should call `enqueue()`.
* **Gateway Validation**: Validate bearer tokens or API keys in your Esmerald/FastAPI gateway before calling `enqueue`.

#### Example (Esmerald):

```python
{!> ../docs_src/tutorial/security/example.py !}
```

### 1.2. Who Can Process Jobs?

* **Worker Isolation**: Run workers in dedicated network segments or namespaces.
* **Credentials**: Workers should authenticate to backends (Redis, Postgres) using strong secrets—never wide-open ACLs.

Example Redis ACL:

```text
# redis.conf
guaclfile /etc/redis/acl.conf
```

```text
# acl.conf
user worker on >StrongPassword ~perf:* +get +set +xread +xreadgroup
```

---

## 2. Encryption (In Transit & At Rest)

### 2.1. In Transit

* **TLS for Redis**: Use `rediss://` URIs and valid certificates.
* **TLS for Postgres**: Enforce `sslmode=require` in connection strings.

Example:

```python
RedisBackend(redis_url="rediss://redis.example.com:6379/0?ssl_cert_reqs=required")
```

### 2.2. At Rest

* **Disk Encryption**: Use encrypted volumes for Redis or Postgres data directories.
* **S3 Encryption**: If using S3 as a custom backend, enable SSE (server-side encryption).

---

## 3. Secure Backend Configuration

### 3.1. Least Privilege

* **Redis**: Create distinct users per queue or environment.
* **Postgres**: Grant only necessary `SELECT`, `INSERT`, `DELETE` on job tables.

### 3.2. Network Controls

* **VPC Peering/Subnets**: Limit backend access to worker subnets.
* **Firewall Rules**: Block public access to databases and Redis.

---

## 4. Audit Logging & Tamper Evidence

### 4.1. Job Lifecycle Logging

* Enable detailed logging: `logging_level = "DEBUG"` in settings.
* Persist logs to centralized systems (ELK, Splunk).

### 4.2. Immutable Audit Trails

* Use append-only logs or Kafka for job events via `event_emitter`.
* Store event payloads with timestamps and signature (HMAC) for tamper detection.

Example HMAC signing:

```python
{!> ../docs_src/tutorial/security/event.py !}
```

---

## 5. Compliance Considerations

### 5.1. GDPR

* **Data Minimization**: Avoid storing PII in job payloads—store references (IDs).
* **Right to Erasure**: Implement a `clean()` strategy to purge user-specific jobs/data.

### 5.2. HIPAA

* **Transmission Security**: Enforce TLS and secure channels.
* **Encryption at Rest**: Mandated for PHI stored in job payloads.

### 5.3. PCI DSS

* **No Card Data in Payloads**: Instead, reference tokens stored in vaults.
* **Access Controls**: Strict IAM roles for services interacting with payment-related queues.

---

## 6. Best Practices & Common Pitfalls

* **Rotate Secrets Regularly**: Use Vault/Secret Manager with short TTLs.
* **Never Log Secrets**: Filter `enqueue` parameters before logging.
* **Monitor ACL Violations**: Set up alerts when unauthorized commands hit Redis/Postgres.
* **Test DR Scenarios**: Simulate key rotation, backend failover, and ensure workers reconnect seamlessly.

!!! Joke
    **Parting joke**: Security is like toothpaste, easy to squirt, hard to put back. 🦷

---

With security and compliance in place, your AsyncMQ pipelines are ready for any audit or pen-test.
