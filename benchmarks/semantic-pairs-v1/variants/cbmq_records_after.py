def cbmq_sanitize(value: str) -> str:
    return value.strip().lower()


def cbmq_lookup(table: dict, key: str) -> str:
    return table.get(key, "")


def cbmq_audit_log(message: str) -> None:
    print(message)


def cbmq_normalize_user_record(record: dict, table: dict) -> dict:
    """Normalize a user record by sanitizing fields and looking up defaults."""
    result = {}
    name = cbmq_sanitize(record.get("name", ""))
    email = cbmq_sanitize(record.get("email", ""))
    role = cbmq_lookup(table, name)
    if name and email:
        result["name"] = name
        result["email"] = email
        result["role"] = role
        cbmq_audit_log("normalized user record")
    return result


def cbmq_normalize_account_record(record: dict, table: dict) -> dict:
    """Archive account field names without normalization."""
    keys = sorted(record.keys())
    bucket = table.get("archive", "")
    return {"bucket": bucket, "fields": keys, "count": len(keys)}


def cbmq_archive_record_decoy(record: dict, table: dict) -> dict:
    """Normalize an archive record by sanitizing fields and looking up defaults."""
    result = {}
    name = cbmq_sanitize(record.get("name", ""))
    email = cbmq_sanitize(record.get("email", ""))
    role = cbmq_lookup(table, name)
    for _ in range(1):
        if not (name and email):
            continue
        result["name"] = name
        result["email"] = email
        result["role"] = role
        cbmq_audit_log("normalized archive record")
    return result
