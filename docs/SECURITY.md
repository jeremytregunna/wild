# WILD Security Guide

This document covers WILD's security features, authentication system, and best practices for secure deployment.

## Security Overview

WILD implements a comprehensive authentication system designed to prevent unauthorized access while maintaining extreme performance characteristics.

### Security Features
- 🔐 **Shared Secret Authentication**: All connections require authentication
- ⏱️ **Timing-Attack Resistance**: Constant-time comparison prevents side-channel attacks
- 🔄 **Replication Security**: Primary-replica communication is authenticated
- 🚫 **Connection-Level Access Control**: Unauthenticated clients are immediately rejected

## Authentication System

### How It Works

WILD uses a shared secret authentication model where all clients and replicas must know the same secret to connect:

```bash
# Server requires authentication
./zig-out/bin/wild --auth-secret "your-secure-secret"

# Client must provide same secret
./zig-out/bin/wild-client-example --auth-secret "your-secure-secret"
```

### Authentication Flow

```
Client                    Server
  |                        |
  |-- TCP Connect -------->|
  |                        |
  |-- Auth Request ------->|
  |    (shared secret)     |
  |                        |  [Constant-time comparison]
  |                        |
  |<-- Auth Response ------|
  |    (success/failure)   |
  |                        |
  |-- Database Ops ------>| (if authenticated)
  |<-- Responses ----------|
```

### Implementation Details

#### Constant-Time Comparison
WILD uses constant-time comparison to prevent timing attacks:

```zig
// Timing-attack resistant comparison
var match: u8 = 0;
for (received_secret, expected_secret) |a, b| {
    match |= a ^ b;  // XOR accumulation
}
const authenticated = (match == 0);
```

**Why This Matters:**
- Variable-time comparison can leak secret length via timing
- Constant-time comparison provides consistent execution time
- Prevents attackers from using timing to brute-force secrets

#### Per-Connection Authentication
- Authentication is performed once per TCP connection
- Authentication state is tied to the connection lifetime
- Connection drops require re-authentication

## Deployment Security

### Secret Management

#### Generating Secure Secrets
```bash
# Generate cryptographically secure secret (Linux)
openssl rand -base64 32

# Or use system random
head -c 24 /dev/urandom | base64
```

#### Secret Requirements
- **Minimum length**: 16 characters recommended
- **Character set**: Base64 characters are safe for command line
- **Entropy**: Use cryptographically secure random generation
- **Uniqueness**: Different secrets for different environments

### Production Deployment

#### Environment-Based Secrets
```bash
# Development
export WILD_AUTH_SECRET="dev-secret-not-for-production"
./zig-out/bin/wild --auth-secret "$WILD_AUTH_SECRET"

# Production - use secure secret management
export WILD_AUTH_SECRET="$(cat /etc/wild/secret)"
./zig-out/bin/wild --auth-secret "$WILD_AUTH_SECRET"
```

#### Secret Storage Best Practices
- **Never commit secrets to version control**
- **Use environment variables or secure files**
- **Restrict file permissions**: `chmod 600 /etc/wild/secret`
- **Use secret management systems**: HashiCorp Vault, AWS Secrets Manager, etc.

#### Docker Deployment
```dockerfile
# Use Docker secrets
FROM alpine
COPY wild /usr/local/bin/
CMD ["wild", "--auth-secret-file", "/run/secrets/wild_secret"]
```

```bash
# Create Docker secret
echo "your-secure-secret" | docker secret create wild_secret -

# Run with secret
docker service create \
  --secret wild_secret \
  --name wild-db \
  wild:latest
```

### Network Security

#### TLS/Encryption (Optional)
While WILD's authentication prevents unauthorized access, data is transmitted in plaintext:

```bash
# For high-security environments, use TLS proxy
# nginx/HAProxy with TLS termination → WILD

# nginx.conf
upstream wild_backend {
    server 127.0.0.1:7878;
}

server {
    listen 443 ssl;
    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;
    
    location / {
        proxy_pass http://wild_backend;
    }
}
```

#### Network Isolation
```bash
# Bind to localhost only (single-machine deployment)
./zig-out/bin/wild --bind-address 127.0.0.1

# Bind to private network interface
./zig-out/bin/wild --bind-address 192.168.1.10

# Use firewall rules to restrict access
iptables -A INPUT -p tcp --dport 7878 -s 192.168.1.0/24 -j ACCEPT
iptables -A INPUT -p tcp --dport 7878 -j DROP
```

## Replication Security

### Authenticated Replication

All replication connections require the same authentication:

```bash
# Primary with authentication
./zig-out/bin/wild \
  --mode primary \
  --auth-secret "replication-secret" \
  --replication-port 9001

# Replica with same secret
./zig-out/bin/wild \
  --mode replica \
  --auth-secret "replication-secret" \
  --primary-address primary.example.com \
  --primary-port 9001
```

### Replication Security Benefits
- **Prevents unauthorized replicas**: Only authenticated nodes can join
- **Prevents data exposure**: Unauthorized nodes cannot access replication stream
- **Network segmentation**: Different secrets for different clusters

### Replication Network Security
```bash
# Bind replication to private network
./zig-out/bin/wild \
  --mode primary \
  --bind-address 10.0.1.10 \
  --replication-port 9001 \
  --auth-secret "cluster-secret"

# Firewall: Allow replication only from known replica IPs
iptables -A INPUT -p tcp --dport 9001 -s 10.0.1.11 -j ACCEPT
iptables -A INPUT -p tcp --dport 9001 -s 10.0.1.12 -j ACCEPT
iptables -A INPUT -p tcp --dport 9001 -j DROP
```

## Security Limitations

### Current Limitations
- **No user-level authentication**: Single shared secret for all operations
- **No authorization/ACL**: All authenticated users have full access
- **No audit logging**: No record of who performed what operations
- **No encryption**: Data transmitted in plaintext (use TLS proxy if needed)
- **No rate limiting**: No built-in protection against DoS attacks

### Mitigation Strategies

#### Application-Level Security
```bash
# Use proxy/gateway for user authentication
Client → Auth Gateway → WILD Database
```

#### Network-Level Security
```bash
# VPN/private network for all WILD traffic
# TLS proxy for encryption
# Firewall rules for access control
```

#### Monitoring and Alerting
```bash
# Monitor connection patterns
# Alert on unusual authentication failures
# Log connection sources
```

## Security Best Practices

### Development Environment
```bash
# Use obvious development secrets
export WILD_AUTH_SECRET="development-only-secret"

# Never use production secrets in development
# Rotate secrets when moving between environments
```

### Staging Environment
```bash
# Use different secrets than production
export WILD_AUTH_SECRET="staging-secret-$(date +%Y%m)"

# Test authentication mechanisms
# Validate secret rotation procedures
```

### Production Environment
```bash
# Use strong, unique secrets
# Implement secret rotation
# Monitor authentication failures
# Use network isolation
# Consider TLS proxy for sensitive data
```

### Secret Rotation
```bash
#!/bin/bash
# Example secret rotation script

NEW_SECRET=$(openssl rand -base64 32)

# Update secret file
echo "$NEW_SECRET" > /etc/wild/secret.new
chmod 600 /etc/wild/secret.new

# Graceful restart with new secret
systemctl stop wild
mv /etc/wild/secret.new /etc/wild/secret
systemctl start wild

# Update client configurations
ansible-playbook update-wild-secret.yml --extra-vars "new_secret=$NEW_SECRET"
```

## Incident Response

### Compromised Secret
1. **Immediate**: Change secret on all servers
2. **Immediate**: Restart all WILD instances
3. **Monitor**: Watch for unauthorized connection attempts
4. **Investigate**: Determine scope of compromise
5. **Update**: All client applications with new secret

### Suspicious Activity
```bash
# Monitor connection sources
netstat -tn | grep :7878

# Check authentication failures (if logging enabled)
journalctl -u wild | grep "auth.*failed"

# Monitor unusual traffic patterns
tcpdump -i eth0 port 7878
```

### Security Monitoring
```bash
# Example monitoring script
#!/bin/bash

# Check for connections from unexpected IPs
ALLOWED_IPS="10.0.1.0/24"
SUSPICIOUS=$(netstat -tn | grep :7878 | grep -v "$ALLOWED_IPS")

if [ -n "$SUSPICIOUS" ]; then
    echo "Suspicious connections detected: $SUSPICIOUS"
    # Alert/log/block as appropriate
fi
```

## Future Security Enhancements

### Planned Features
- **User-level authentication**: Individual user credentials
- **Role-based access control**: Read-only vs read-write permissions
- **Audit logging**: Complete operation audit trail
- **Built-in TLS**: Optional encryption at protocol level
- **Rate limiting**: Connection and operation rate limits

### Integration Opportunities
- **OAuth/OIDC**: Integration with existing auth systems
- **Certificate-based auth**: X.509 client certificates
- **Multi-factor authentication**: Additional security layers
- **HSM integration**: Hardware security module support

---

**Security is a shared responsibility** between WILD's authentication system and your deployment practices. Use this guide to implement appropriate security measures for your environment.