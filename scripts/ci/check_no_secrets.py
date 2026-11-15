"""Fail if any tracked file contains something that looks like a credential.

Complements gitleaks in CI with checks specific to this project: real AWS
account IDs in ARNs, Snowflake account URLs, private keys, API tokens and
Slack webhooks. Placeholders such as <AWS_ACCOUNT_ID> are fine.

    python scripts/ci/check_no_secrets.py           # scan all git-tracked files
    python scripts/ci/check_no_secrets.py file ...  # scan specific files
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

PATTERNS = {
    "AWS access key id": re.compile(r"\b(AKIA|ASIA)[0-9A-Z]{16}\b"),
    "AWS secret key assignment": re.compile(r"aws_secret_access_key\s*[=:]\s*['\"]?[A-Za-z0-9/+]{40}"),
    "AWS account id in ARN": re.compile(r"arn:aws:[a-z0-9-]+:[a-z0-9-]*:\d{12}:"),
    "private key block": re.compile(r"-----BEGIN (RSA |EC |OPENSSH |ENCRYPTED )?PRIVATE KEY-----"),
    "Snowflake account URL": re.compile(r"\b[a-z0-9]+-[a-z0-9_]+\.snowflakecomputing\.com\b", re.I),
    "Snowflake RSA public key value": re.compile(r"RSA_PUBLIC_KEY\s*=\s*'MII"),
    "Anthropic API key": re.compile(r"\bsk-ant-[A-Za-z0-9_-]{20,}"),
    "Slack webhook": re.compile(r"https://hooks\.slack\.com/services/T[A-Z0-9]+/B[A-Z0-9]+/[A-Za-z0-9]+"),
    "Slack token": re.compile(r"\bxox[abposr]-[A-Za-z0-9-]{10,}"),
    "GitHub token": re.compile(r"\bgh[pousr]_[A-Za-z0-9]{36,}"),
}

# This file defines the patterns, so it would match itself.
SKIP = {"scripts/ci/check_no_secrets.py"}


def tracked_files() -> list[str]:
    out = subprocess.run(["git", "ls-files"], capture_output=True, text=True, check=True)
    return out.stdout.splitlines()


def scan(paths: list[str]) -> list[str]:
    findings = []
    for path in paths:
        if path in SKIP:
            continue
        p = Path(path)
        if not p.is_file():
            continue
        try:
            text = p.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue  # binary files
        for lineno, line in enumerate(text.splitlines(), 1):
            for label, rx in PATTERNS.items():
                if rx.search(line):
                    findings.append(f"{path}:{lineno}: possible {label}")
    return findings


def main(argv: list[str]) -> int:
    findings = scan(argv or tracked_files())
    for f in findings:
        print(f)
    if findings:
        print(f"\n{len(findings)} possible secret(s). Move the value to airflow/.env (see .env.example).")
        return 1
    print("No secrets found.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
