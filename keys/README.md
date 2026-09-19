# keys

Generated locally. Nothing here is committed except this file.

    openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out keys/rsa_key.p8 -nocrypt
    openssl rsa -in keys/rsa_key.p8 -pubout -out keys/rsa_key.pub

Paste the body of `rsa_key.pub` (no BEGIN/END lines) into the
`RSA_PUBLIC_KEY` value in `setup/04_service_user.sql`, then set
`SNOWFLAKE_PRIVATE_KEY_PATH=keys/rsa_key.p8` in `.env`.