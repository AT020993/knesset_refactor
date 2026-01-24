"""Generate Streamlit Cloud secrets file with base64 encoded GCP credentials."""
import json
import base64

# The credentials
creds = {
    "type": "service_account",
    "project_id": "iucc-international-dimensions",
    "private_key_id": "b86f1553b1325cd839df1616f79cda520a8fb515",
    "private_key": """-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC4rzHcXta2X8qK
b17Z0/Dh2gyBY2sexrcYmheuFtDz3OwCrdcN8fPiGX2l4K7S2WyZistvdr9dObh6
Fu81ocY2nl38++Ki+YMMMz18R220hDdyq2ffrmhDjJF6yUeXwQWCqo8bk6kkiG9j
uyZECkB/ZxyGns8BvcwrJm1GueMlSEeDgSHaRaO+mJnLRmNiifoHIXq5itfkaR6j
FhVyMw0bXnRHPjId6l6NkWS5shbDmD7IHlIqVTmvfa9z14VQCPc3vON8NWL1timI
onPLSVUZb7kNN063lZfbclV9RfsNII1H0j89fi7WZdJbPu1nRQunWkwe7VL/a+vY
2jdWWMNfAgMBAAECggEAAZVQPkflMP4O50iUzcBBOJEdJKonW8WvVyYnCCaI7GDw
owXo1I/w8SIaeia8Hkb1OoxpObo7t2m3GPuDryLaZ1Mh53kkqb9YLhgYXFg5FWBB
dhmzE8SpKdRq1jHN4SarTJdNZRqgdhWMrZRdUR2st4rjleHcUMhW7Ohw7QzLPja7
LGEUGjlWfinkYpbiN4bjlPh1Dye0AxGgKaCi/302n3ibYosU3XmoIRAYpLXFUVDO
w6Z6pZSqUuRmqRAETZVKyJ9161DkpyuTS5Sdi3tSeq92TGOesWwYAI7HULc5u5pR
griqChhHZakRMhTbTE6zJeLXv5SeDO/H4B045NbJBQKBgQDsrgr6xIYNSwjmG0M7
kQ8jzLthvAmkCwjlEZT3PPgdQpfcovaa//JiVZ2viUBH8564nH6IzNMBr5EE8rFh
2xMQ9J0Ka+wBk8hYSE0lJVQAy3iRE4up7zX2vOpHDnumHugi7eanpuU9ZLxNXNNA
qBZO/Z2xAajWElqyjkJNW8PvBQKBgQDHwpbQ563LYdfmgEY5ekAVjTvI0gW4jCf/
ldk6tGmXEAhY/0X2GC/oJVHIHG9dd7/FmdtXbNvMNkW4zFOc6xUsF2wmeWZUEFbc
V/3qvY+Vw1KFhwkXNTgsjjChFD/C5YonM1aNJGNBg6069W+G2l7nh/JjLhyifh39
4HVH5BjOEwKBgHG2QOIwj3NNjK35hTjWPd7mW1TeogclohVkeBy9NzvfpdTEmxTn
SY4DMmqG19J58tUhow2Y0vwpXNRywdSRC70GuXirV01+si/wNNhW8eAb4gZZzK1N
l7C/HRQcmj269a7qe/oqjlML1giZQ4n5+BXldeD4OiT31omZHbPLFfz9AoGAD5A3
k9sZDUl4OrsvvXaq24L/b2v9ih1RWHZiUFKSD3TuUZDB8KR9xwZxUrf0bUHRLSIJ
lswia8ymrfktYoJmSeMhR/YZZN9JBS3N2KYgM7jeCw00RhydXctOOp6LQTIFC3zS
d3ioSwhpTLimkckdr8hFuDh1fqt4xrjgKlO5IwUCgYB66fUBGbSfhmIpyO6DEnVN
n5AgIiCvA3onrO7vvy8uW3UTshrRsMJxyf0r9G4j4GstaR8xqXkYV/CONr8lS2iZ
lMCrtvoy0HECkIIDm0gFilLTP0vdDDf1LszO5bBCKMIxlXaLKz52gHOHzTSopTxF
+ukQ46vcOzPqS7+VPcwqBQ==
-----END PRIVATE KEY-----
""",
    "client_email": "knesset@iucc-international-dimensions.iam.gserviceaccount.com",
    "client_id": "102959788703790503008",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/knesset%40iucc-international-dimensions.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
}

# Convert to JSON and base64
json_str = json.dumps(creds, separators=(',', ':'))
b64 = base64.b64encode(json_str.encode()).decode()

# Create TOML content
toml_content = f'''[cap_annotation]
enabled = true
bootstrap_admin_username = "admin"
bootstrap_admin_display_name = "Administrator"
bootstrap_admin_password = "knesset2026"

[storage]
gcs_bucket_name = "knesset_bucket"

[gcp_service_account]
credentials_base64 = "{b64}"
'''

# Write to file
with open('streamlit_secrets_for_cloud.toml', 'w') as f:
    f.write(toml_content)

print("File created: streamlit_secrets_for_cloud.toml")
print(f"Base64 length: {len(b64)} characters")
