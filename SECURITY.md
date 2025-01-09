# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in AresaDB, please report it by sending an email to **yevheniyc@gmail.com**.

Please include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Any suggested fixes

### Response Timeline

- **Initial Response**: Within 48 hours
- **Assessment**: Within 1 week
- **Fix (if applicable)**: Within 2 weeks for critical issues

## Security Considerations

### Local Storage

- **File Permissions**: AresaDB creates database files with default system permissions. Ensure your database directory has appropriate access controls.
- **Encryption at Rest**: Not currently implemented. For sensitive data, use filesystem-level encryption (e.g., LUKS, FileVault, BitLocker).

### Cloud Storage

#### AWS S3

- **Credentials**: Never commit AWS credentials to version control. Use:
  - Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
  - IAM roles (recommended for EC2/ECS)
  - AWS credentials file (`~/.aws/credentials`)

- **Bucket Policies**: Configure appropriate bucket policies:
  ```json
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {"AWS": "arn:aws:iam::ACCOUNT_ID:user/aresadb-user"},
        "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
        "Resource": "arn:aws:s3:::your-bucket/databases/*"
      }
    ]
  }
  ```

- **Encryption**: Enable S3 server-side encryption:
  - SSE-S3 (Amazon S3-managed keys)
  - SSE-KMS (AWS KMS-managed keys)
  - SSE-C (Customer-provided keys)

#### Google Cloud Storage

- **Service Accounts**: Use service accounts with minimal required permissions
- **IAM Roles**: Assign `Storage Object User` role for read/write access
- **Encryption**: GCS encrypts data at rest by default

### Network Security

- **HTTPS**: Cloud storage operations use HTTPS by default
- **Firewall**: No network ports opened by default (local storage only)
- **Server Mode** (future): Will require authentication configuration

### Data Validation

- **Input Sanitization**: JSON properties are parsed and validated
- **SQL Injection**: SQL parser validates query structure
- **Path Traversal**: Database paths are validated to prevent directory traversal

## Best Practices

### Development

```bash
# Use environment variables for credentials
export AWS_ACCESS_KEY_ID=$(cat ~/.secrets/aws_key)
export AWS_SECRET_ACCESS_KEY=$(cat ~/.secrets/aws_secret)

# Never log credentials
aresadb -v push s3://bucket/path  # Verbose mode doesn't log secrets
```

### Production

1. **Principle of Least Privilege**: Grant minimal required permissions
2. **Audit Logging**: Enable cloud provider audit logs
3. **Backup**: Regular backups with tested restore procedures
4. **Updates**: Keep AresaDB and dependencies updated

### Configuration

```toml
# ~/.config/aresadb/config.toml

# Don't store credentials here!
# Use environment variables or IAM roles

[defaults]
format = "table"
```

## Known Limitations

1. **No built-in encryption**: Data stored in plain format
2. **No authentication**: Local database has no access control
3. **No audit logging**: Operations not logged by default

## Future Security Enhancements

- [ ] At-rest encryption option
- [ ] Authentication for server mode
- [ ] Audit logging
- [ ] Role-based access control (RBAC)
- [ ] Client certificate authentication

## Dependencies

AresaDB depends on security-critical crates:
- `object_store`: Cloud storage (audited, Apache project)
- `redb`: Local storage (audited, well-tested)
- `tokio`: Async runtime (widely used, security-conscious)

Dependencies are regularly updated for security patches.

---

For general questions, please use [GitHub Discussions](https://github.com/yoreai/aresadb/discussions).

